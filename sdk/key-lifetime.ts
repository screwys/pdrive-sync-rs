// SPDX-License-Identifier: MIT
import type {
    KeyReference,
    WorkerGenerateKeyOptions,
    WorkerImportPrivateKeyOptions,
    WorkerImportPublicKeyOptions,
    WorkerReformatKeyOptions,
} from '@protontech/crypto';
import { Api as CryptoApi } from '@protontech/crypto/proxy/endpoint/api.ts';

type KeyData = string | Uint8Array<ArrayBuffer>;

export class LifetimeCryptoApi extends CryptoApi {
    private keyFinalizers = this.createFinalizers();

    private createFinalizers(): FinalizationRegistry<KeyReference> {
        const finalizers = new FinalizationRegistry<KeyReference>((key) => {
            if (this.keyFinalizers === finalizers) void super.clearKey({ key });
        });
        return finalizers;
    }

    private track<T extends KeyReference>(key: T): T {
        // Keep the endpoint key only while an SDK or account reference is alive.
        this.keyFinalizers.register(key, { ...key }, key);
        return key;
    }

    override async importPrivateKey<T extends KeyData>(options: WorkerImportPrivateKeyOptions<T>, index?: number) {
        return this.track(await super.importPrivateKey(options, index));
    }

    override async importPublicKey<T extends KeyData>(options: WorkerImportPublicKeyOptions<T>, index?: number) {
        return this.track(await super.importPublicKey(options, index));
    }

    override async generateKey<
        CustomConfig extends { v6Keys?: boolean; aeadProtect?: boolean } | undefined = {},
    >(options: WorkerGenerateKeyOptions<CustomConfig>) {
        return this.track(await super.generateKey(options));
    }

    override async reformatKey(options: WorkerReformatKeyOptions) {
        return this.track(await super.reformatKey(options));
    }

    override async cloneKeyAndChangeUserIDs(options: Parameters<CryptoApi['cloneKeyAndChangeUserIDs']>[0]) {
        return this.track(await super.cloneKeyAndChangeUserIDs(options));
    }

    override async clearKey(options: { key: KeyReference }) {
        this.keyFinalizers.unregister(options.key);
        return super.clearKey(options);
    }

    override async clearKeyStore() {
        this.keyFinalizers = this.createFinalizers();
        return super.clearKeyStore();
    }
}
