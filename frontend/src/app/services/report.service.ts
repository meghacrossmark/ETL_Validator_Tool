import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Observable } from 'rxjs';

@Injectable({
  providedIn: 'root'
})
export class ReportService {

  constructor(private http: HttpClient) { }
  
  // getJsonData(): Observable<any> {
  //   // Path is relative to the assets folder
  //   return this.http.get<any>('./assets/data.json');
  // }
}
