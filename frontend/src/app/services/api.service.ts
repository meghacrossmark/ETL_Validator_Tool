import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable } from 'rxjs';

@Injectable({
  providedIn: 'root'
})
export class ApiService {
  // private baseUrl = 'http://172.27.211.164:5000';
  private baseUrl = 'http://localhost:5000';
  constructor(private http: HttpClient) {}

  connect(params: any): Observable<any> {
    return this.http.post(`${this.baseUrl}/connect`, params);
  }

  updateTable(params: any): Observable<any> {
    return this.http.post(`${this.baseUrl}/updateTable`, params);
  }

  performValidation(params: any): Observable<any> {
    return this.http.post(`${this.baseUrl}/perform_validation`, params);
  }

  reset(): Observable<any> {
    return this.http.post(`${this.baseUrl}/reset`, {});
  }

  uploadFile(type: 'source' | 'target', formData: FormData): Observable<any> {
    return this.http.post(`${this.baseUrl}/${type}File`, formData);
  }

  getHeaders(type: 'source' | 'target'): Observable<any> {
    return this.http.post(`${this.baseUrl}/get_headers`, { connection_type: type });
  }

  run_query(query: String): Observable<any> {
    return this.http.post(`${this.baseUrl}/run_query`, { query });
  }
  
  // getReportsData(): Observable<any> {
  //   return this.http.get(`${this.baseUrl}/reports_data`);
  // }

}