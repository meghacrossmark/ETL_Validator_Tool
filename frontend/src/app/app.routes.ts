// app.router.ts
import { Routes } from '@angular/router';
import { LayoutComponent } from './components/layout/layout.component';
//import { HistoryComponent } from './components/pages/history/history.component';
import { PerformComponent } from './components/pages/perform/perform.component';
import { ReportComponent } from './components/pages/report/report.component';
import { HistoryComponent } from './components/pages/history/history.component';
import { UsersComponent } from './components/pages/users/users.component';
import { LoginComponent } from './components/pages/login/login.component';
import { QueryBuilderComponent } from './components/pages/query-builder/query-builder.component';
//import { ConfigComponent } from './components/pages/config/config.component';
//import { UsersComponent } from './components/pages/users/users.component';

export const routes: Routes = [
  {
    path: '',
    component: LayoutComponent,
    children: [
      { path: '', redirectTo: 'perform', pathMatch: 'full' },
      { 
        path: 'reports', 
        component: ReportComponent,
      },
      { 
        path: 'perform', 
        component: PerformComponent
      },
      // { 
      //   path: 'login', 
      //   component: LoginComponent
      // },
      // { 
      //   path: 'history', 
      //   component: HistoryComponent
      // },
      // { 
      //   path: 'users', 
      //   component: UsersComponent
      // },
      { 
        path: 'query', 
        component: QueryBuilderComponent
      },
    ]
  }
];