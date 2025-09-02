// import { CommonModule } from "@angular/common";
// import { Component,ElementRef, ViewChild, Input, OnChanges, SimpleChanges, Output, EventEmitter } from "@angular/core";
// import { FormBuilder, FormGroup, ReactiveFormsModule } from "@angular/forms";
// import { ApiService } from "@services/api.service";
// import { Connection } from "app/models/connection.model";


// @Component({
//   selector: 'app-connection-form',
//   standalone: true,
//   imports: [CommonModule,ReactiveFormsModule],
//   templateUrl: './connection-form.component.html',
//   styleUrls: ['./connection-form.component.scss']
// })
// export class ConnectionFormComponent implements OnChanges {
//   @Input() type : 'source' | 'target' = 'source';
//   @Input() operation : 'quality' | 'compare' = 'quality';
//   @Input() selectedTestToPerform : string= '';
//   primary_connection_text : string = 'Select Connection';

//   @ViewChild('fileInput') fileInput!: ElementRef;
//   isTxtFileUploaded: boolean = false;
//   isConnectBtnVisible = false;
//   // Add this property to your ConnectionFormComponent class
//   @Output() fileUploadCompleted = new EventEmitter<boolean>();
//   @Output() formReady = new EventEmitter<boolean>();
//   columnHeaders: any[] = [];
//   dataTypes:any[]=[];
//   connectionForm: FormGroup;
//   isFileUploaded = false;
//   uploadErrorMessage: string | null = null;

//   delimiterOptions = [
//     { value: ',', label: 'Comma (,)' },
//     { value: '\t', label: 'Tab (\\t)' },
//     { value: ' ', label: 'Space ( )' },
//     { value: ';', label: 'Semicolon (;)' },
//     { value: '|', label: 'Pipe (|)' },
//     { value: 'F', label: 'F (F)' },
//     { value: 'Φ', label: 'Phi (Φ)' },
//     { value: 'þ', label: 'Thorn (þ)' },
//     { value: 'Φ', label: 'NCHAR(0x03A6) (Φ)' }
//   ];

//   constructor(private fb: FormBuilder, private apiService: ApiService) {
//     document.querySelector(`#FileUploadContainer button`)?.classList.remove("uploaded")
//     this.connectionForm = this.fb.group({
//       connectionType: [''],
      
//       selected_fileName :[''],
//       seperator: [','],

//       table: [''],
//       serverName: [''],
//       dbName: [''],

//       tableDB: [''],
//       serverHostName: [''],
//       httpPath: [''],
//       accessToken: [''],
//       batchId: [''],
//       dbSchemaName: [''],

//       username_snowflake: [''],
//       password_snowflake: [''],
//       account_snowflake: [''],
//       warehouse_snowflake: [''],
//       database_snowflake: [''],
//       schema_snowflake: [''],
//       tableName_snowflake: [''],
//       header: ['all'],

//       accountUrl: [''],
//       sasToken: [''],
//       containerName: [''],
//       blobName:['']
//     });
//   }

//   ngOnChanges(changes: SimpleChanges): void {
//     this.setPrimaryConnectionText();
//     console.log(changes);
    
//   }

//   ngOnInit(): void {
//     this.connectionForm.valueChanges.subscribe(() => {
//       this.formReady.emit(this.isFormReady);
//     });
//   }
  
//   get isFormReady(): boolean {
//     const connType = this.connectionForm.get('connectionType')?.value;

//     switch (connType) {
//       case 'databricks':
//         return this.connectionForm.get('serverHostName')?.value &&
//                this.connectionForm.get('httpPath')?.value &&
//                this.connectionForm.get('accessToken')?.value &&
//                this.connectionForm.get('batchId')?.value &&
//                this.connectionForm.get('dbSchemaName')?.value &&
//                this.connectionForm.get('tableDB')?.value;

//       case 'MSSQL':
//         return this.connectionForm.get('serverName')?.value &&
//                this.connectionForm.get('dbName')?.value &&
//                this.connectionForm.get('table')?.value;

//       case 'file_upload':
//         return this.connectionForm.get('selected_fileName')?.value &&
//                this.isFileUploaded;

//       case 'snowflake':
//         return this.connectionForm.get('username_snowflake')?.value &&
//                this.connectionForm.get('password_snowflake')?.value &&
//                this.connectionForm.get('account_snowflake')?.value &&
//                this.connectionForm.get('warehouse_snowflake')?.value &&
//                this.connectionForm.get('database_snowflake')?.value &&
//                this.connectionForm.get('schema_snowflake')?.value &&
//                this.connectionForm.get('tableName_snowflake')?.value;

//       case 'azure_blob_storage':
//         return this.connectionForm.get('accountUrl')?.value &&
//                this.connectionForm.get('sasToken')?.value &&
//                this.connectionForm.get('containerName')?.value &&
//                this.connectionForm.get('blobName')?.value;

//       default:
//         return false;
//     }
//   }

//   private setPrimaryConnectionText(): void {
//     if (this.operation === 'compare') {
//       this.primary_connection_text = this.type === 'source'
//         ? 'Select Source Connection'
//         : 'Select Target Connection';
//     } else {
//       this.primary_connection_text = 'Select Connection';
//     }
//   }

//   triggerFileUpload() {
//     this.fileInput.nativeElement.click();
//     //this.isConnectBtnVisible = true;

//   }
// // before code applying any extension for file upload
//   // onFileSelected(event: Event) {
//   //   let button: any = null;
//   //   const input = event.target as HTMLInputElement;
//   //   if (input.files && input.files[0]) {
//   //     const file = input.files[0];

//   //     const extension = file.name.split('.').pop()?.toLowerCase();
//   //     const isTxt = extension === 'txt';

//   //     this.isTxtFileUploaded = isTxt;

//   //     const formData = new FormData();
//   //     formData.append(`${this.type}_file`, file);
//   //     const buttons = document.querySelectorAll(`#FileUploadContainer button`);

//   //     if(this.type=='target'){
//   //        button = buttons[1]; // Index starts at 0
//   //     }
//   //     else{
//   //        button = buttons[0];
//   //     }
//   //     if (button) {
//   //       button.textContent = 'Uploading File....';
//   //       button.classList.remove('uploaded');
//   //       button.classList.add('uploading');
//   //     }
//   //     this.apiService.uploadFile(this.type, formData).subscribe({
//   //       next: (data) => {
//   //         if (button) {
//   //          this.isFileUploaded = true;
//   //           button.textContent = `Uploaded File: ${data[`${this.type}_fileName`] ?? 'File Not Uploaded/ Extension Error'}`;
//   //           button.classList.remove('uploading');
//   //           button.classList.add('uploaded');
//   //           // After successful file upload, emit the event
//   //           this.fileUploadCompleted.emit(true);
//   //         }
//   //         this.connectionForm.get('selected_fileName')?.setValue(data[`${this.type}_fileName`] ?? undefined);
//   //         this.columnHeaders = data.headers || [];

//   //       },
//   //       error: (error) => console.error('Error:', error)
//   //     });
//   //   }
//   // }
  
//   onFileSelected(event: Event) {
//     let button: any = null;
//     this.uploadErrorMessage = null; // Clear previous error
  
//     const input = event.target as HTMLInputElement;
//     if (input.files && input.files[0]) {
//       const file = input.files[0];
//       const extension = file.name.split('.').pop()?.toLowerCase();
  
//       const allowedExtensions = ['csv', 'xlsx', 'txt'];
//       if (!allowedExtensions.includes(extension || '')) {
//         this.uploadErrorMessage = 'Unsupported file type. Please upload a .csv, .xlsx, or .txt file.';
//         return;
//       }
  
//       this.isTxtFileUploaded = extension === 'txt';
  
//       const formData = new FormData();
//       formData.append(`${this.type}_file`, file);
  
//       const buttons = document.querySelectorAll(`#FileUploadContainer button`);
//       button = this.type === 'target' ? buttons[1] : buttons[0];
  
//       if (button) {
//         button.textContent = 'Uploading File....';
//         button.classList.remove('uploaded');
//         button.classList.add('uploading');
//       }
  
//       this.apiService.uploadFile(this.type, formData).subscribe({
//         next: (data) => {
//           if (button) {
//             this.isFileUploaded = true;
//             button.textContent = `Uploaded File: ${data[`${this.type}_fileName`] ?? 'File Not Uploaded/ Extension Error'}`;
//             button.classList.remove('uploading');
//             button.classList.add('uploaded');
//             this.fileUploadCompleted.emit(true);
//           }
  
//           this.connectionForm.get('selected_fileName')?.setValue(data[`${this.type}_fileName`] ?? undefined);
//           this.columnHeaders = data.headers || [];
//         },
//         error: (error) => {
//           this.uploadErrorMessage = 'Failed to upload file. Please try again.';
//           console.error('Error:', error);
//         }
//       });
//     }
//   }
  
  
//   onConnectionTypeChange() {
//     this.columnHeaders = [];


//   }

//   createConnection(): Connection {
//       const formValue = this.connectionForm.value;
//       const connType = formValue.connectionType;
//       let connParams: Record<string, any> = {};
//       let tblName: string | undefined;
//       let headers: any[] = formValue.header === 'all' ? this.columnHeaders : [formValue.header];
//       let dataTypes: any[] = formValue.dataType === 'all' ? this.dataTypes : [formValue.dataType];
//       let tblPrimaryKey: string | undefined = formValue.selectColumn !== 'all' ? formValue.selectColumn : undefined;
  
//       switch (connType) {
//         case 'file_upload':
//           connParams = {
//             selected_fileName: formValue.selected_fileName,
//             separator: formValue.seperator 
//           };
//           break;
//         case 'MSSQL':
//           connParams = {
//             serverName: formValue.serverName,
//             dbName: formValue.dbName,
//             table: formValue.table
//           };
//           tblName = formValue.table;
//           break;
//         case 'databricks':
//           connParams = {
//             serverHostName: formValue.serverHostName,
//             httpPath: formValue.httpPath,
//             accessToken: formValue.accessToken,
//             batchId: formValue.batchId,
//             dbSchemaName: formValue.dbSchemaName,
//             tableDB: formValue.tableDB
//           };
//           tblName = formValue.tableDB;
//           break;
//           case 'azure_blob_storage':
//             connParams = {
//              accountUrl: formValue.accountUrl,
//              sasToken: formValue.sasToken,
//              containerName: formValue.containerName,
//              blobName: formValue.blobName
//            };
//            break;
//         case 'snowflake':
//           connParams = {
//             username: formValue.username_snowflake,
//             password: formValue.password_snowflake,
//             account: formValue.account_snowflake,
//             warehouse: formValue.warehouse_snowflake,
//             database: formValue.database_snowflake,
//             schema: formValue.schema_snowflake
//           };
//           tblName = formValue.tableName_snowflake;
//           break;
//       }
//       return {
//         conn: this.type,
//         conn_type: connType,
//         conn_params: connParams,
//         tbl_name: tblName,
//         headers: headers.length > 0 ? headers : undefined,
//         tbl_primary_key: tblPrimaryKey
//       };
//     }

//     reset(): void {
//       // Clear the form
//       this.connectionForm.reset({
//         connectionType: '',
//         selected_fileName: '',
//         seperator: ',',
//         table: '',
//         serverName: '',
//         dbName: '',
//         tableDB: '',
//         serverHostName: '',
//         httpPath: '',
//         accessToken: '',
//         batchId: '',
//         dbSchemaName: '',
//         username_snowflake: '',
//         password_snowflake: '',
//         account_snowflake: '',
//         warehouse_snowflake: '',
//         database_snowflake: '',
//         schema_snowflake: '',
//         tableName_snowflake: '',
//         header: 'all'
//       });
    
//       // Reset component state
//       this.columnHeaders = [];
//       this.isTxtFileUploaded = false;
//       this.isConnectBtnVisible = false;
    
//       // Reset upload button styles (if needed)
//       const buttons = document.querySelectorAll(`#FileUploadContainer button`);
//       buttons.forEach(btn => {
//         btn.textContent = 'Upload File';
//         btn.classList.remove('uploading', 'uploaded');
//       });
//     }
    
// }


import { CommonModule } from "@angular/common";
import { Component, ElementRef, ViewChild, Input, OnChanges, SimpleChanges, Output, EventEmitter } from "@angular/core";
import { FormBuilder, FormGroup, ReactiveFormsModule, Validators } from "@angular/forms";
import { ApiService } from "@services/api.service";
import { Connection } from "app/models/connection.model";

@Component({
  selector: 'app-connection-form',
  standalone: true,
  imports: [CommonModule, ReactiveFormsModule],
  templateUrl: './connection-form.component.html',
  styleUrls: ['./connection-form.component.scss']
})
export class ConnectionFormComponent implements OnChanges {
  @Input() type: 'source' | 'target' = 'source';
  @Input() operation: 'quality' | 'compare' = 'quality';
  @Input() selectedTestToPerform: string = '';
  primary_connection_text: string = 'Select Connection';

  @ViewChild('fileInput') fileInput!: ElementRef;
  isTxtFileUploaded: boolean = false;
  isConnectBtnVisible = false;
  @Output() fileUploadCompleted = new EventEmitter<boolean>();
  @Output() formReady = new EventEmitter<boolean>();
  columnHeaders: any[] = [];
  dataTypes: any[] = [];
  connectionForm: FormGroup;
  isFileUploaded = false;
  uploadErrorMessage: string | null = null;

  delimiterOptions = [
    { value: ',', label: 'Comma (,)' },
    { value: '\t', label: 'Tab (\\t)' },
    { value: ' ', label: 'Space ( )' },
    { value: ';', label: 'Semicolon (;)' },
    { value: '|', label: 'Pipe (|)' },
    { value: 'F', label: 'F (F)' },
    { value: 'Φ', label: 'Phi (Φ)' },
    { value: 'þ', label: 'Thorn (þ)' },
    { value: 'Φ', label: 'NCHAR(0x03A6) (Φ)' }
  ];

  constructor(private fb: FormBuilder, private apiService: ApiService) {
    document.querySelector(`#FileUploadContainer button`)?.classList.remove("uploaded")
    this.connectionForm = this.fb.group({
      connectionType: ['', Validators.required],
      
      selected_fileName: [''],
      seperator: [','],

      table: [''],
      serverName: [''],
      dbName: [''],

      tableDB: [''],
      serverHostName: [''],
      httpPath: [''],
      accessToken: [''],
      batchId: [''], // No validation for batch ID as requested
      dbSchemaName: [''],

      username_snowflake: [''],
      password_snowflake: [''],
      account_snowflake: [''],
      warehouse_snowflake: [''],
      database_snowflake: [''],
      schema_snowflake: [''],
      tableName_snowflake: [''],
      header: ['all'],

      accountUrl: [''],
      sasToken: [''],
      containerName: [''],
      blobName: ['']
    });
  }

  ngOnChanges(changes: SimpleChanges): void {
    this.setPrimaryConnectionText();
    console.log(changes);
  }

  ngOnInit(): void {
    this.connectionForm.valueChanges.subscribe(() => {
      this.formReady.emit(this.isFormReady);
    });
  }

  // Method to check if a specific field has validation errors
  isFieldInvalid(fieldName: string): boolean {
    const field = this.connectionForm.get(fieldName);
    return !!(field && field.invalid && (field.dirty || field.touched));
  }

  // Method to get validation error message for a field
  getFieldError(fieldName: string): string {
    const field = this.connectionForm.get(fieldName);
    if (field?.errors) {
      if (field.errors['required']) {
        return `${this.getFieldDisplayName(fieldName)} is required`;
      }
      if (field.errors['minlength']) {
        return `${this.getFieldDisplayName(fieldName)} must be at least ${field.errors['minlength'].requiredLength} characters`;
      }
    }
    return '';
  }

  // Helper method to get user-friendly field names
  private getFieldDisplayName(fieldName: string): string {
    const fieldNames: { [key: string]: string } = {
      'serverHostName': 'Server Hostname',
      'httpPath': 'HTTP Path',
      'accessToken': 'Access Token',
      'dbSchemaName': 'DB Schema Name',
      'tableDB': 'Table',
      'serverName': 'Server Name',
      'dbName': 'Database Name',
      'table': 'Table',
      'username_snowflake': 'Username',
      'password_snowflake': 'Password',
      'account_snowflake': 'Account',
      'warehouse_snowflake': 'Warehouse',
      'database_snowflake': 'Database',
      'schema_snowflake': 'Schema',
      'tableName_snowflake': 'Table Name',
      'accountUrl': 'Account URL',
      'sasToken': 'SAS Token',
      'containerName': 'Container Name',
      'blobName': 'Blob Name'
    };
    return fieldNames[fieldName] || fieldName;
  }

  get isFormReady(): boolean {
    const connType = this.connectionForm.get('connectionType')?.value;

    switch (connType) {
      case 'databricks':
        return this.connectionForm.get('serverHostName')?.valid &&
               this.connectionForm.get('httpPath')?.valid &&
               this.connectionForm.get('accessToken')?.valid &&
               this.connectionForm.get('dbSchemaName')?.valid &&
               this.connectionForm.get('tableDB')?.valid &&
               // Note: batchId is not validated as per requirement
               this.connectionForm.get('serverHostName')?.value &&
               this.connectionForm.get('httpPath')?.value &&
               this.connectionForm.get('accessToken')?.value &&
               this.connectionForm.get('dbSchemaName')?.value &&
               this.connectionForm.get('tableDB')?.value;

      case 'MSSQL':
        return this.connectionForm.get('serverName')?.valid &&
               this.connectionForm.get('dbName')?.valid &&
               this.connectionForm.get('table')?.valid &&
               this.connectionForm.get('serverName')?.value &&
               this.connectionForm.get('dbName')?.value &&
               this.connectionForm.get('table')?.value;

      case 'file_upload':
        return this.connectionForm.get('selected_fileName')?.value &&
               this.isFileUploaded;

      case 'snowflake':
        return this.connectionForm.get('username_snowflake')?.valid &&
               this.connectionForm.get('password_snowflake')?.valid &&
               this.connectionForm.get('account_snowflake')?.valid &&
               this.connectionForm.get('warehouse_snowflake')?.valid &&
               this.connectionForm.get('database_snowflake')?.valid &&
               this.connectionForm.get('schema_snowflake')?.valid &&
               this.connectionForm.get('tableName_snowflake')?.valid &&
               this.connectionForm.get('username_snowflake')?.value &&
               this.connectionForm.get('password_snowflake')?.value &&
               this.connectionForm.get('account_snowflake')?.value &&
               this.connectionForm.get('warehouse_snowflake')?.value &&
               this.connectionForm.get('database_snowflake')?.value &&
               this.connectionForm.get('schema_snowflake')?.value &&
               this.connectionForm.get('tableName_snowflake')?.value;

      case 'azure_blob_storage':
        return this.connectionForm.get('accountUrl')?.valid &&
               this.connectionForm.get('sasToken')?.valid &&
               this.connectionForm.get('containerName')?.valid &&
               this.connectionForm.get('blobName')?.valid &&
               this.connectionForm.get('accountUrl')?.value &&
               this.connectionForm.get('sasToken')?.value &&
               this.connectionForm.get('containerName')?.value &&
               this.connectionForm.get('blobName')?.value;

      default:
        return false;
    }
  }

  private setPrimaryConnectionText(): void {
    if (this.operation === 'compare') {
      this.primary_connection_text = this.type === 'source'
        ? 'Select Source Connection'
        : 'Select Target Connection';
    } else {
      this.primary_connection_text = 'Select Connection';
    }
  }

  triggerFileUpload() {
    this.fileInput.nativeElement.click();
  }

  onFileSelected(event: Event) {
    let button: any = null;
    this.uploadErrorMessage = null;

    const input = event.target as HTMLInputElement;
    if (input.files && input.files[0]) {
      const file = input.files[0];
      const extension = file.name.split('.').pop()?.toLowerCase();

      const allowedExtensions = ['csv', 'xlsx', 'txt'];
      if (!allowedExtensions.includes(extension || '')) {
        this.uploadErrorMessage = 'Unsupported file type. Please upload a .csv, .xlsx, or .txt file.';
        return;
      }

      this.isTxtFileUploaded = extension === 'txt';

      const formData = new FormData();
      formData.append(`${this.type}_file`, file);

      const buttons = document.querySelectorAll(`#FileUploadContainer button`);
      button = this.type === 'target' ? buttons[1] : buttons[0];

      if (button) {
        button.textContent = 'Uploading File....';
        button.classList.remove('uploaded');
        button.classList.add('uploading');
      }

      this.apiService.uploadFile(this.type, formData).subscribe({
        next: (data) => {
          if (button) {
            this.isFileUploaded = true;
            button.textContent = `Uploaded File: ${data[`${this.type}_fileName`] ?? 'File Not Uploaded/ Extension Error'}`;
            button.classList.remove('uploading');
            button.classList.add('uploaded');
            this.fileUploadCompleted.emit(true);
          }

          this.connectionForm.get('selected_fileName')?.setValue(data[`${this.type}_fileName`] ?? undefined);
          this.columnHeaders = data.headers || [];
        },
        error: (error) => {
          this.uploadErrorMessage = 'Failed to upload file. Please try again.';
          console.error('Error:', error);
        }
      });
    }
  }

  onConnectionTypeChange() {
    this.columnHeaders = [];
    this.setValidationRules();
  }

  private setValidationRules(): void {
    const connType = this.connectionForm.get('connectionType')?.value;
    
    // Clear all existing validators first
    Object.keys(this.connectionForm.controls).forEach(key => {
      if (key !== 'connectionType' && key !== 'batchId') { // Keep connectionType validation, skip batchId
        this.connectionForm.get(key)?.clearValidators();
        this.connectionForm.get(key)?.updateValueAndValidity();
      }
    });

    // Apply validators based on connection type
    switch (connType) {
      case 'databricks':
        this.connectionForm.get('serverHostName')?.setValidators([Validators.required]);
        this.connectionForm.get('httpPath')?.setValidators([Validators.required]);
        this.connectionForm.get('accessToken')?.setValidators([Validators.required]);
        this.connectionForm.get('dbSchemaName')?.setValidators([Validators.required]);
        this.connectionForm.get('tableDB')?.setValidators([Validators.required]);
        // Note: batchId intentionally has no validators
        break;

      case 'MSSQL':
        this.connectionForm.get('serverName')?.setValidators([Validators.required]);
        this.connectionForm.get('dbName')?.setValidators([Validators.required]);
        this.connectionForm.get('table')?.setValidators([Validators.required]);
        break;

      case 'snowflake':
        this.connectionForm.get('username_snowflake')?.setValidators([Validators.required]);
        this.connectionForm.get('password_snowflake')?.setValidators([Validators.required]);
        this.connectionForm.get('account_snowflake')?.setValidators([Validators.required]);
        this.connectionForm.get('warehouse_snowflake')?.setValidators([Validators.required]);
        this.connectionForm.get('database_snowflake')?.setValidators([Validators.required]);
        this.connectionForm.get('schema_snowflake')?.setValidators([Validators.required]);
        this.connectionForm.get('tableName_snowflake')?.setValidators([Validators.required]);
        break;

      case 'azure_blob_storage':
        this.connectionForm.get('accountUrl')?.setValidators([Validators.required]);
        this.connectionForm.get('sasToken')?.setValidators([Validators.required]);
        this.connectionForm.get('containerName')?.setValidators([Validators.required]);
        this.connectionForm.get('blobName')?.setValidators([Validators.required]);
        break;
    }

    // Update validity after setting validators
    Object.keys(this.connectionForm.controls).forEach(key => {
      if (key !== 'batchId') {
        this.connectionForm.get(key)?.updateValueAndValidity();
      }
    });
  }

  createConnection(): Connection {
    const formValue = this.connectionForm.value;
    const connType = formValue.connectionType;
    let connParams: Record<string, any> = {};
    let tblName: string | undefined;
    let headers: any[] = formValue.header === 'all' ? this.columnHeaders : [formValue.header];
    let dataTypes: any[] = formValue.dataType === 'all' ? this.dataTypes : [formValue.dataType];
    let tblPrimaryKey: string | undefined = formValue.selectColumn !== 'all' ? formValue.selectColumn : undefined;

    switch (connType) {
      case 'file_upload':
        connParams = {
          selected_fileName: formValue.selected_fileName,
          separator: formValue.seperator
        };
        break;
      case 'MSSQL':
        connParams = {
          serverName: formValue.serverName,
          dbName: formValue.dbName,
          table: formValue.table
        };
        tblName = formValue.table;
        break;
      case 'databricks':
        connParams = {
          serverHostName: formValue.serverHostName,
          httpPath: formValue.httpPath,
          accessToken: formValue.accessToken,
          batchId: formValue.batchId,
          dbSchemaName: formValue.dbSchemaName,
          tableDB: formValue.tableDB
        };
        tblName = formValue.tableDB;
        break;
      case 'azure_blob_storage':
        connParams = {
          accountUrl: formValue.accountUrl,
          sasToken: formValue.sasToken,
          containerName: formValue.containerName,
          blobName: formValue.blobName
        };
        break;
      case 'snowflake':
        connParams = {
          username: formValue.username_snowflake,
          password: formValue.password_snowflake,
          account: formValue.account_snowflake,
          warehouse: formValue.warehouse_snowflake,
          database: formValue.database_snowflake,
          schema: formValue.schema_snowflake
        };
        tblName = formValue.tableName_snowflake;
        break;
    }
    return {
      conn: this.type,
      conn_type: connType,
      conn_params: connParams,
      tbl_name: tblName,
      headers: headers.length > 0 ? headers : undefined,
      tbl_primary_key: tblPrimaryKey
    };
  }

  reset(): void {
    this.connectionForm.reset({
      connectionType: '',
      selected_fileName: '',
      seperator: ',',
      table: '',
      serverName: '',
      dbName: '',
      tableDB: '',
      serverHostName: '',
      httpPath: '',
      accessToken: '',
      batchId: '',
      dbSchemaName: '',
      username_snowflake: '',
      password_snowflake: '',
      account_snowflake: '',
      warehouse_snowflake: '',
      database_snowflake: '',
      schema_snowflake: '',
      tableName_snowflake: '',
      header: 'all'
    });

    this.columnHeaders = [];
    this.isTxtFileUploaded = false;
    this.isConnectBtnVisible = false;

    const buttons = document.querySelectorAll(`#FileUploadContainer button`);
    buttons.forEach(btn => {
      btn.textContent = 'Upload File';
      btn.classList.remove('uploading', 'uploaded');
    });
  }
}