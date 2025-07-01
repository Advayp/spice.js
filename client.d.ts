import { Table } from 'apache-arrow';
import { RefreshOverrides, type SpiceClientConfig } from './interfaces';
declare class SpiceClient {
    private _apiKey?;
    private _flightUrl;
    private _httpUrl;
    private _userAgent;
    private _flightTlsEnabled;
    private _maxRetries;
    constructor(params?: string | SpiceClientConfig);
    private createClient;
    private getResultStream;
    query(queryText: string, onData?: ((data: Table) => void) | undefined): Promise<Table>;
    private doQueryRequest;
    setMaxRetries(maxRetries: number): void;
    refreshDataset(dataset: string, refresh_overrides?: RefreshOverrides): Promise<void>;
    private fetchInternal;
}
export { SpiceClient };
