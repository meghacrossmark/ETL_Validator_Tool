import {Connection,Test} from './connection.model' 

export interface Report {
    type_of_test: 'quality' | 'compare';
    name_of_test: Test['operation'];
    test_owner: number;
    conn_metadata: Connection & {conn: 'source'};
    target_conn_metadata?: Connection & {conn: 'target'};
    result_metadata: ResultMetadata;
  }
  
export interface ResultMetadata {
    status: boolean;
    matched_rows: number;
    failed_rows: number;
    source: SourceTargetDetails;
    target?: SourceTargetDetails;
    execution_output: any[];
  }
  
export interface SourceTargetDetails {
    rows: number;
    columns?: number;
    column?: number;
    not_in_target?: number;
    not_in_source?: number;
    details: string[];
  }