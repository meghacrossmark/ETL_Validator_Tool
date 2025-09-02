// src/app/services/page-info.service.ts
import { Injectable } from '@angular/core';
import { BehaviorSubject } from 'rxjs';

@Injectable({
  providedIn: 'root'
})
export class PageInfoService {
  private pageNameSubject = new BehaviorSubject<string>('Dashboard');
  private toolNameSubject = new BehaviorSubject<string>('Tool Name');
  
  pageName$ = this.pageNameSubject.asObservable();
  toolName$ = this.toolNameSubject.asObservable();
  
  updatePageName(pageName: string) {
    this.pageNameSubject.next(pageName);
  }
  
  updateToolName(toolName: string) {
    this.toolNameSubject.next(toolName);
  }
}