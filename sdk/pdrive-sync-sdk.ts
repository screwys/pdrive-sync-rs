// SPDX-License-Identifier: MIT
import { createHash } from 'node:crypto';
import { lstat, mkdir, open, unlink } from 'node:fs/promises';
import { homedir } from 'node:os';
import path from 'node:path';
import { createInterface } from 'node:readline';

import { Database } from 'bun:sqlite';

import { CryptoProxy } from '@protontech/crypto';
import { Api as CryptoApi } from '@protontech/crypto/proxy/endpoint/api.ts';
import {
    DriveEventType,
    FeatureFlags,
    NodeType,
    NodeWithSameNameExistsValidationError,
    OpenPGPCryptoWithCryptoProxy,
    ProtonDriveClient,
    ValidationError,
    type NodeEntity,
} from '@protontech/drive-sdk';
import { Telemetry } from '@protontech/drive-sdk/telemetry';

import { initApi } from './api';
import { createCaches } from './cache';
import { splitPathSegments } from './cli/paths';
import { getOrGenerateClientUid } from './clientUid';
import { getConfig } from './config';
import { initCredentials } from './credentials';
import { NoEventsProvider } from './events/providerNoEvents';

declare const APP_VERSION: string;
declare const SDK_VERSION: string;

type UploadFile = {
    path: string;
    sha1?: string | null;
    expectedRemote?: { state: 'absent' } | { state: 'revision'; uid: string; revisionUid: string } | null;
};
type ParentLocation = { root: string; parentComponents: string[] };
type TrashTarget = ParentLocation & {
    uid: string;
    parentUid: string;
    name: string;
    revisionUid?: string | null;
};
type Request =
    | { method: 'info' | 'list'; path: string }
    | ({ method: 'create_folder'; parent: string; name: string } & ParentLocation)
    | { method: 'child'; parent: string; name: string }
    | { method: 'events'; scope: string; cursor?: string | null }
    | ({ method: 'upload'; parent: string; files: UploadFile[] } & ParentLocation)
    | { method: 'download'; path: string; parent: string; sha1: string; size: number; revisionUid?: string }
    | { method: 'trash'; targets: TrashTarget[] }
    | { method: 'exit' | 'reset_cache' };

if (process.argv.includes('--version')) {
    process.stdout.write(`pdrive-sync-sdk ${APP_VERSION.split('@')[1]}\n`);
    process.exit(0);
}

async function init() {
    const telemetry = new Telemetry({ logHandlers: [], metricHandlers: [] });
    const logger = telemetry.getLogger('pdrive-sync');
    const config = getConfig({
        appVersion: APP_VERSION,
        sdkVersion: SDK_VERSION,
        clientUidPrefix: 'pdrive-sync',
    });
    // Credentials keep Proton's paths and keyring names.
    const credentials = initCredentials(config, logger);
    const sdkCacheDir = process.env.PDRIVE_SYNC_SDK_CACHE_DIR || path.join(
        process.env.XDG_CACHE_HOME || path.join(homedir(), '.cache'), 'pdrive-sync-sdk',
    );
    const sdkAppDir = path.join(
        process.env.XDG_DATA_HOME || path.join(homedir(), '.local', 'share'), 'pdrive-sync-sdk',
    );
    const sdkConfig = { ...config, cacheDir: sdkCacheDir, appDir: sdkAppDir };
    await Promise.all([
        mkdir(sdkCacheDir, { recursive: true, mode: 0o700 }),
        mkdir(sdkAppDir, { recursive: true, mode: 0o700 }),
    ]);
    CryptoApi.init({});
    CryptoProxy.setEndpoint(new CryptoApi(), (endpoint) => endpoint.clearKeyStore());
    const { auth, addresses, srp, httpClient } = await initApi(config, credentials, logger, CryptoProxy);
    if (!auth.isLoggedIn()) {
        throw new Error('Log in with proton-drive before syncing');
    }
    const caches = createCaches(sdkConfig, credentials, logger);
    // Proton's cache removes tags by key when it updates an entity.
    for (const file of ['cache-crypto.sqlite', 'cache-entities.sqlite']) {
        const db = new Database(path.join(sdkCacheDir, file));
        db.run('CREATE INDEX IF NOT EXISTS entities_labels_key ON entities_labels(key)');
        db.close();
    }
    const sdk = new ProtonDriveClient({
        config: { baseUrl: config.baseUrl, clientUid: await getOrGenerateClientUid(sdkConfig, logger) },
        httpClient,
        account: addresses,
        srpModule: srp,
        openPGPCryptoModule: new OpenPGPCryptoWithCryptoProxy(CryptoProxy),
        entitiesCache: caches.entitiesCache,
        cryptoCache: caches.cryptoCache,
        telemetry,
        latestEventIdProvider: new NoEventsProvider(),
        featureFlagProvider: {
            async isEnabled(flag) {
                return flag === FeatureFlags.DriveSmallFileUpload;
            },
        },
    });
    return { sdk, clearCache: () => caches.entitiesCache.clear() };
}

let client: ReturnType<typeof init> | undefined;
const latestCursors = new Map<string, string>();

async function getClient() {
    if (!client) {
        client = init().catch(() => {
            client = undefined;
            throw new Error('SDK initialization failed. Check Proton Drive login and credentials store');
        });
    }
    return client;
}

function normalize(node: NodeEntity) {
    const revision = node.activeRevision;
    return {
        uid: node.uid,
        parentUid: node.parentUid,
        treeEventScopeId: node.treeEventScopeId,
        name: node.name.ok ? node.name : { ok: false },
        type: node.type,
        totalStorageSize: node.totalStorageSize,
        activeRevision: revision && {
            uid: revision.uid,
            storageSize: revision.storageSize as number | undefined,
            claimedSize: revision.claimedSize,
            claimedModificationTime: revision.claimedModificationTime,
            claimedDigests: revision.claimedDigests,
        },
    };
}

function isUid(value: string) {
    return /^([a-zA-Z0-9=_-]{88,108}|[a-zA-Z0-9_-]{22})~([a-zA-Z0-9=_-]{88,108}|[a-zA-Z0-9_-]{22})$/.test(value);
}

async function byUid(sdk: ProtonDriveClient, uid: string) {
    for await (const node of sdk.iterateNodes([uid])) {
        return 'missingUid' in node ? null : node;
    }
    throw new Error('SDK did not return a node result');
}

async function* children(sdk: ProtonDriveClient, parent: NodeEntity) {
    let uids: string[] = [];
    for await (const uid of sdk.iterateFolderChildrenNodeUids(parent)) {
        uids.push(uid);
        if (uids.length < 100) continue;
        for await (const node of sdk.iterateNodes(uids)) {
            if ('missingUid' in node) {
                throw new Error('Remote folder changed during listing');
            }
            yield node;
        }
        uids = [];
    }
    for await (const node of sdk.iterateNodes(uids)) {
        if ('missingUid' in node) {
            throw new Error('Remote folder changed during listing');
        }
        yield node;
    }
}

async function byName(sdk: ProtonDriveClient, parent: NodeEntity, name: string) {
    for await (const node of children(sdk, parent)) {
        if (!node.name.ok) {
            throw new Error('Cannot read a remote filename');
        }
        if (node.name.value === name) {
            return node;
        }
    }
    return null;
}

async function resolve(sdk: ProtonDriveClient, remotePath: string): Promise<NodeEntity | null> {
    if (isUid(remotePath)) {
        return byUid(sdk, remotePath);
    }
    const [, section, ...parts] = splitPathSegments(remotePath);
    let node: NodeEntity | null = null;
    if (section === 'my-files') {
        node = await sdk.getMyFilesRootFolder();
    } else if (section === 'devices') {
        const name = parts.shift();
        for await (const device of sdk.iterateDevices()) {
            if (!device.name.ok) {
                throw new Error('Cannot read a remote device name');
            }
            if (device.name.value === name) {
                node = await byUid(sdk, device.rootFolderUid);
                break;
            }
        }
    } else if (section === 'shared-with-me') {
        const name = parts.shift();
        for await (const shared of sdk.iterateSharedNodesWithMe()) {
            if (!shared.name.ok) {
                throw new Error('Cannot read a shared folder name');
            }
            if (shared.name.value === name) {
                node = shared;
                break;
            }
        }
    } else {
        throw new ValidationError('Unsupported remote path');
    }
    for (const part of parts) {
        if (!node) break;
        if (part === '') continue;
        node = isUid(part) ? await byUid(sdk, part) : await byName(sdk, node, part);
    }
    return node;
}

async function requiredNode(sdk: ProtonDriveClient, remotePath: string) {
    const node = await resolve(sdk, remotePath);
    if (!node) throw new ValidationError('Remote node not found');
    return node;
}

async function refreshEvents(sdk: ProtonDriveClient, scope: string) {
    const cursor = latestCursors.get(scope);
    if (!cursor) return;
    for await (const event of sdk.iterateEvents(scope, cursor)) {
        if (event.type === DriveEventType.TreeRemove) {
            latestCursors.delete(scope);
            throw new Error('Remote tree is no longer available');
        }
        latestCursors.set(scope, event.eventId);
    }
}

async function intendedParent(sdk: ProtonDriveClient, parentUid: string, location: ParentLocation) {
    let node = await resolve(sdk, location.root);
    for (const name of location.parentComponents) {
        if (!node) return null;
        node = await byName(sdk, node, name);
    }
    return node?.uid === parentUid ? node : null;
}

async function upload(sdk: ProtonDriveClient, request: Extract<Request, { method: 'upload' }>) {
    const savedParent = await requiredNode(sdk, request.parent);
    await refreshEvents(sdk, savedParent.treeEventScopeId);
    const parent = await intendedParent(sdk, savedParent.uid, request);
    if (!parent) throw new Error('Remote folder changed during sync');
    const existing = new Map<string, NodeEntity>();
    for await (const node of children(sdk, parent)) {
        if (!node.name.ok) throw new Error('Cannot read a remote filename');
        existing.set(node.name.value, node);
    }
    const report = {
        transferredItems: 0, skippedItems: 0, transferredBytes: 0,
        failures: [] as { name: string; error: string }[], nodes: [] as ReturnType<typeof normalize>[],
    };
    let next = 0;
    const worker = async () => {
        while (next < request.files.length) {
            const file = request.files[next++];
            const name = path.basename(file.path);
            try {
                const metadata = await lstat(file.path);
                if (!metadata.isFile()) throw new ValidationError('Upload source is not a regular file');
                const local = Bun.file(file.path);
                let sha1 = file.sha1?.toLowerCase();
                if (!sha1) {
                    const hash = createHash('sha1');
                    for await (const chunk of local.stream()) hash.update(chunk);
                    sha1 = hash.digest('hex');
                }
                const prior = existing.get(name);
                const expected = file.expectedRemote;
                if (expected && (expected.state === 'absent' ? !!prior :
                    prior?.uid !== expected.uid || prior.activeRevision?.uid !== expected.revisionUid)) {
                    throw new Error('Remote file changed during sync');
                }
                if (prior?.activeRevision?.claimedDigests?.sha1?.toLowerCase() === sha1 &&
                    prior.activeRevision.claimedSize === metadata.size) {
                    report.skippedItems++;
                    report.nodes.push(normalize(prior));
                    continue;
                }
                if (prior && prior.type !== NodeType.File) throw new Error('Remote name belongs to a folder');
                const settings = {
                    mediaType: local.type || 'application/octet-stream', expectedSize: metadata.size,
                    expectedSha1: sha1, modificationTime: metadata.mtime,
                };
                const uploader = prior
                    ? await sdk.getFileRevisionUploader(prior, settings)
                    : await sdk.getFileUploader(parent, name, settings);
                const controller = await uploader.uploadFromStream(local.stream(), []);
                const receipt = await controller.completion();
                report.transferredItems++;
                report.transferredBytes += metadata.size;
                // A server event may not be visible yet. The receipt names the committed revision.
                report.nodes.push({
                    uid: receipt.nodeUid, parentUid: parent.uid, treeEventScopeId: parent.treeEventScopeId,
                    name: { ok: true, value: name }, type: NodeType.File,
                    totalStorageSize: undefined,
                    activeRevision: {
                        uid: receipt.nodeRevisionUid, storageSize: undefined, claimedSize: metadata.size,
                        claimedModificationTime: metadata.mtime,
                        claimedDigests: { sha1, sha1Verified: true },
                    },
                });
            } catch (error) {
                report.failures.push({ name, error: errorMessage(error) });
            }
        }
    };
    await Promise.all(Array.from({ length: Math.min(5, request.files.length) }, worker));
    return report;
}

async function download(sdk: ProtonDriveClient, request: Extract<Request, { method: 'download' }>) {
    const node = await requiredNode(sdk, request.path);
    if (!node.name.ok) throw new Error('Cannot read a remote filename');
    const name = node.name.value;
    if (path.basename(name) !== name || name === '.' || name === '..') {
        throw new ValidationError('Invalid remote filename');
    }
    const destination = path.join(request.parent, name);
    const downloader = request.revisionUid
        ? await sdk.getFileRevisionDownloader(request.revisionUid)
        : await sdk.getFileDownloader(node);
    const file = await open(destination, 'wx', 0o600);
    const hash = createHash('sha1');
    let size = 0;
    try {
        const stream = new WritableStream<Uint8Array>({
            async write(chunk) {
                hash.update(chunk);
                size += chunk.byteLength;
                await file.writeFile(chunk);
            },
        });
        await downloader.downloadToStream(stream).completion();
        if (size !== request.size || hash.digest('hex') !== request.sha1.toLowerCase()) {
            throw new Error('Downloaded file does not match the expected size and SHA1');
        }
    } catch (error) {
        await file.close();
        await unlink(destination);
        throw error;
    }
    await file.close();
    return null;
}

async function dispatch(request: Request) {
    const known = ['info', 'list', 'child', 'create_folder', 'events', 'upload', 'download', 'trash', 'reset_cache'];
    if (!known.includes(request.method)) throw new Error('Unknown SDK method');
    const { sdk, clearCache } = await getClient();
    switch (request.method) {
        case 'reset_cache':
            await clearCache();
            latestCursors.clear();
            return null;
        case 'info': {
            const node = await resolve(sdk, request.path);
            return node ? normalize(node) : null;
        }
        case 'list': return Array.fromAsync(children(sdk, await requiredNode(sdk, request.path)), normalize);
        case 'child': {
            const node = await byName(sdk, await requiredNode(sdk, request.parent), request.name);
            return node ? normalize(node) : null;
        }
        case 'create_folder': {
            const savedParent = await requiredNode(sdk, request.parent);
            await refreshEvents(sdk, savedParent.treeEventScopeId);
            const parent = await intendedParent(sdk, savedParent.uid, request);
            if (!parent) throw new Error('Remote folder changed during sync');
            try {
                return normalize(await sdk.createFolder(parent, request.name));
            } catch (error) {
                if (!(error instanceof NodeWithSameNameExistsValidationError) || !error.existingNodeUid) throw error;
                const node = await sdk.getNode(error.existingNodeUid);
                if (node.type !== NodeType.Folder || node.parentUid !== parent.uid ||
                    !node.name.ok || node.name.value !== request.name) throw error;
                return normalize(node);
            }
        }
        case 'events': {
            const result = {
                cursor: request.cursor || '', events: [] as object[], refresh: false, removed: false,
            };
            for await (const event of sdk.iterateEvents(request.scope, request.cursor ?? undefined)) {
                result.cursor = event.eventId;
                if (event.type === DriveEventType.TreeRefresh) result.refresh = true;
                if (event.type === DriveEventType.TreeRemove) {
                    result.removed = true;
                    break;
                }
                if ('nodeUid' in event) {
                    result.events.push({
                        type: event.type, nodeUid: event.nodeUid, parentNodeUid: event.parentNodeUid,
                        isTrashed: 'isTrashed' in event ? event.isTrashed : undefined,
                    });
                }
            }
            if (!result.cursor) throw new Error('SDK did not return an event cursor');
            if (result.removed) latestCursors.delete(request.scope);
            else latestCursors.set(request.scope, result.cursor);
            return result;
        }
        case 'upload': return upload(sdk, request);
        case 'download': return download(sdk, request);
        case 'trash': {
            const result = { succeededUids: [] as string[], failedUids: [] as string[] };
            const targets: string[] = [];
            const refreshed = new Set<string>();
            const parents = new Map<string, NodeEntity | null>();
            for (const target of request.targets) {
                const key = JSON.stringify([target.root, target.parentComponents, target.parentUid]);
                if (!parents.has(key)) {
                    const savedParent = await byUid(sdk, target.parentUid);
                    if (savedParent && !refreshed.has(savedParent.treeEventScopeId)) {
                        await refreshEvents(sdk, savedParent.treeEventScopeId);
                        refreshed.add(savedParent.treeEventScopeId);
                    }
                    parents.set(key, savedParent ? await intendedParent(sdk, target.parentUid, target) : null);
                }
                const parent = parents.get(key);
                if (!parent) {
                    result.failedUids.push(target.uid);
                    continue;
                }
                const node = await byUid(sdk, target.uid);
                if (!node || node.parentUid !== parent.uid || !node.name.ok || node.name.value !== target.name ||
                    target.revisionUid && node.activeRevision?.uid !== target.revisionUid) {
                    result.failedUids.push(target.uid);
                    continue;
                }
                targets.push(target.uid);
            }
            for await (const node of sdk.trashNodes(targets)) {
                (node.ok ? result.succeededUids : result.failedUids).push(node.uid);
            }
            return result;
        }
    }
}

function errorMessage(error: unknown): string {
    return error instanceof Error ? error.message : 'SDK operation failed';
}

const lines = createInterface({ input: process.stdin, crlfDelay: Infinity });
for await (const line of lines) {
    let response: object;
    let exiting = false;
    try {
        const request = JSON.parse(line) as Request;
        exiting = request.method === 'exit';
        response = { value: exiting ? null : await dispatch(request) };
    } catch (error) {
        response = { error: error instanceof SyntaxError ? 'Invalid JSON request' : errorMessage(error) };
    }
    await new Promise<void>((resolve, reject) => {
        process.stdout.write(`${JSON.stringify(response)}\n`, (error) => error ? reject(error) : resolve());
    });
    if (exiting) break;
}
process.exit(0);
