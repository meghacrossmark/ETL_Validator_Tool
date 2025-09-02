import { Component } from '@angular/core';
import { FormBuilder, FormGroup, FormsModule, ReactiveFormsModule, Validators } from '@angular/forms';

@Component({
  selector: 'app-login',
  imports: [FormsModule, ReactiveFormsModule],
  templateUrl: './login.component.html',
  styleUrl: './login.component.scss'
})
export class LoginComponent {
  loginForm: FormGroup;
  loading = false;
  submitted = false;

  constructor(private formBuilder: FormBuilder) { }

  ngOnInit(): void {
    this.loginForm = this.formBuilder.group({
      email: ['', [Validators.required, Validators.email]]
    });
  }

  // Convenience getter for easy access to form fields
  get f() { return this.loginForm.controls; }

  onSubmit(): void {
    this.submitted = true;

    // Stop here if form is invalid
    if (this.loginForm.invalid) {
      return;
    }

    this.loading = true;
    
    // Here you would typically call your authentication service
    // For example:
    // this.authService.login(this.f.email.value)
    //   .pipe(first())
    //   .subscribe(
    //     data => {
    //       // Handle successful login
    //       this.router.navigate(['/dashboard']);
    //     },
    //     error => {
    //       // Handle error
    //       this.loading = false;
    //     });
    
    // For demonstration purposes, simulate an API call
    // setTimeout(() => {
    //   console.log('Login attempt with email:', this.loginForm.value.email);
    //   this.loading = false;
    //   // Here you would redirect to another page on success
    // }, 1500);
  }
}
