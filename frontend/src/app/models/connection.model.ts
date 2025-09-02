export interface Connection {
  conn_name?: string;
  conn: 'source' | 'target';
  conn_type: string;
  conn_params: Record<string, any>;
  tbl_name?: string;
  headers?: any[];
  dataTypes?: string[];
  tbl_primary_key?: string;
  query?: string;
}

export class Test {
  logged_in_user_id: number;
  operation: string;
  test_params:  Record<string, any>;
  test_type: string;
}

