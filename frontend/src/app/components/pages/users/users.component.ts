import { CommonModule } from '@angular/common';
import { Component } from '@angular/core';
import { User } from 'app/models/users.model';

@Component({
  selector: 'app-users',
  imports: [ CommonModule],
  templateUrl: './users.component.html',
  styleUrl: './users.component.scss'
})
export class UsersComponent {

  users: User[] = [
    {
      username: 'john_doe',
      email: 'john.doe@example.com',
      role: 'Admin',
      isActive: true
    },
    {
      username: 'jane_smith',
      email: 'jane.smith@example.com',
      role: 'Viewer',
      isActive: true
    },
    {
      username: 'mike_wilson',
      email: 'mike.wilson@example.com',
      role: 'Performer',
      isActive: false
    },
    {
      username: 'sarah_johnson',
      email: 'sarah.johnson@example.com',
      role: 'Viewer',
      isActive: true
    },
    {
      username: 'david_brown',
      email: 'david.brown@example.com',
      role: 'Admin',
      isActive: false
    },
    {
      username: 'lisa_davis',
      email: 'lisa.davis@example.com',
      role: 'Viewer',
      isActive: true
    }
  ];

  getRoleClass(role: string): string {
    return `role-${role.toLowerCase()}`;
  }
}
