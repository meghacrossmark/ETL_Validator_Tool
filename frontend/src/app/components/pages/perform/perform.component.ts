import { Component, ViewChild, ElementRef, ChangeDetectorRef, HostListener, ViewChildren, QueryList } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormBuilder, FormGroup, ReactiveFormsModule } from '@angular/forms';
import { HttpClientModule } from '@angular/common/http';
import { ApiService } from '@services/api.service';
import { QueryBuilderComponent } from "../query-builder/query-builder.component";
import { Connection, Test } from 'app/models/connection.model';
import { FunctionDefinition } from 'app/models/operations.model';
import { ConnectionFormComponent } from 'app/components/common_components/connection-form/connection-form.component';
import { ReportComponent } from '../report/report.component';
import { ToastrModule, ToastrService } from 'ngx-toastr';


@Component({
  selector: 'app-perform',
  standalone: true,
  imports: [CommonModule, ReactiveFormsModule, HttpClientModule, QueryBuilderComponent, ConnectionFormComponent, ReportComponent],
  templateUrl: './perform.component.html',
  styleUrls: ['./perform.component.scss']
})

export class PerformComponent {

  columns = [
    { name: 'id', type: 'INT' },
    { name: 'name', type: 'VARCHAR' },
    { name: 'date', type: 'TIMESTAMP' }
  ];

  functionList: FunctionDefinition[] = [];
  selectedFunction: any = null;
  selectedParams: any = null;
  data_types: [''];
  sourceConnection: Connection;
  targetConnection: Connection;

  operation: Test = {
    logged_in_user_id: 0,
    operation: '',
    test_params: [],
    test_type: ''
  };

  // Add this property to track operations that require key columns
  private operationsRequiringKeyColumns = [
    'check_duplicate',
    'check_data_type',
    'compare_data',
    'check_null'
  ];

  isTxtFileUploadedSource: boolean = false;
  isTxtFileUploadedTarget: boolean = false;

  connectionForm: FormGroup;
  dynamicForm: FormGroup;
  showTargetConnection = false;
  sourceQuery: string | undefined = undefined;
  targetQuery: string | undefined = undefined;
  resultHeader: string | undefined = undefined;
  result: string | undefined = undefined;
  connectButtonText = 'Connect';
  showModal = false;
  selectedTable: string | null = null;
  queryType: 'source' | 'target' = 'source';
  selectedSourceFile: string | undefined = undefined;
  selectedTargetFile: string | undefined = undefined;
  sourceHeaders: any[] = [];
  targetHeaders: any[] = [];

  isPerformBtnVisible = false;
  isTestParametersVisible = false;
  isConnectBtnVisible = false;
  isSourceQueryVisible = false;
  isTargetQueryVisible = false;
  isReportVisible = false; // initially hidden
  //isResultHeaderVisible = false;

  resultReports: any = null;
  resultHeaderReports: string = '';
  showReports: boolean = false;
  resetButton = false;
  selectedKeyColumns: string[] = [];

  sourceFormReady = false;
  targetFormReady = false;
  showButtons: boolean = false;

  // New properties for multi-select dropdown
  selectedHeaders: { [paramKey: string]: string[] } = {};
  isDropdownOpen: { [paramKey: string]: boolean } = {};

  paramEntriesResult: any = [];
  // Add timeout properties to prevent infinite spinner
  private downloadTimeoutId: any;
  private readonly DOWNLOAD_TIMEOUT_MS = 10000; // 10 seconds max


  // @ViewChildren('dropdownMenu') dropdownMenus!: QueryList<ElementRef>;

  // scrollPositions: { [key: string]: number } = {};

  @ViewChild('sourceFileInput') sourceFileInput!: ElementRef;
  @ViewChild('targetFileInput') targetFileInput!: ElementRef;

  @ViewChild('sourceConnectionForm') sourceConnectionForm!: ConnectionFormComponent;
  @ViewChild('targetConnectionForm') targetConnectionForm!: ConnectionFormComponent;

  @ViewChild('queryBuilder') queryBuilder!: QueryBuilderComponent;

  public showSpinner: boolean = false;
  // New properties for spinner duration tracking
  public spinnerStartTime: number | null = null;
  public spinnerEndTime: number | null = null;
  public spinnerDuration: number = 0;
  public totalSpinnerTime: number = 0; // Track cumulative spinner time across multiple operations

  constructor(private fb: FormBuilder, private apiService: ApiService, private readonly cdRef: ChangeDetectorRef, private toastr: ToastrService, private cdr: ChangeDetectorRef) {

    this.loadFunctionDefinitions();
    this.connectionForm = this.fb.group({
      operation: [''],
    });
    this.dynamicForm = this.fb.group({});

  }

  //Updated method to handle dynamic forms creation
  onOperationChange(event: Event) {

    const selectedValue = (event.target as HTMLSelectElement).value;
    console.log('Selected operation:', selectedValue);

    // Set visibility based on selected operation
    this.isTestParametersVisible = selectedValue !== 'compare_count';

    // RESET LOGIC: Clear all connection-related data when operation changes
    this.resetConnectionData();

    this.selectedFunction = this.functionList.find(f => f.funct_value === selectedValue);
    console.log(this.selectedFunction);

    this.selectedParams = this.selectedFunction ? this.selectedFunction.funct_params : null;

    this.showTargetConnection = this.selectedFunction?.funct_type === "compare";
    this.targetHeaders = [];

    // Create dynamic form controls
    const formControls: { [key: string]: any } = { operation: [selectedValue] };

    if (this.selectedParams) {
      Object.keys(this.selectedParams).forEach(key => {
        const paramConfig = this.selectedParams[key];
        const paramType = paramConfig[2];
        const defaultValue = paramType === 'CheckBox' ? false : '';
        formControls[key] = [defaultValue];
      });
    }

    this.dynamicForm = this.fb.group(formControls);
    // this.isConnectBtnVisible = true;
    // this.resetButton = true;
  }
  // Updated method to handle dynamic forms creation
  // onOperationChange(event: Event) {
  //   const selectedValue = (event.target as HTMLSelectElement).value;
  //   console.log('Selected operation:', selectedValue);

  //   // Find the selected function
  //   this.selectedFunction = this.functionList.find(f => f.funct_value === selectedValue);
  //   this.selectedParams = this.selectedFunction ? this.selectedFunction.funct_params : null;

  //   // Set visibility based on selected operation
  //   // Hide test parameters ONLY for count comparison operations
  //   // Method 1: Check by function value (adjust these values based on your actual data)
  //   const isCountComparisonOperation = 
  //     selectedValue === 'compare_count' || 
  //     selectedValue === 'count_compare' ||
  //     selectedValue === 'row_count' ||
  //     selectedValue === 'table_count';

  //   // Method 2: Check by function name (more flexible)
  //   const isCountByName = this.selectedFunction && 
  //     this.selectedFunction.funct_name && 
  //     this.selectedFunction.funct_name.toLowerCase().includes('count');

  //   // Method 3: Check if function has no parameters (count operations typically don't need parameters)
  //   const hasNoParameters = !this.selectedParams || Object.keys(this.selectedParams).length === 0;

  //   // Use the most appropriate method based on your data structure
  //   this.isTestParametersVisible = !(isCountComparisonOperation || isCountByName);

  //   // Alternative: Use this if count operations have no parameters
  //   // this.isTestParametersVisible = !hasNoParameters;

  //   console.log('Test parameters visible:', this.isTestParametersVisible);

  //   // RESET LOGIC: Clear all connection-related data when operation changes
  //   this.resetConnectionData();

  //   this.showTargetConnection = this.selectedFunction?.funct_type === "compare";
  //   this.targetHeaders = [];

  //   // Create dynamic form controls
  //   const formControls: { [key: string]: any } = { operation: [selectedValue] };

  //   if (this.selectedParams) {
  //     Object.keys(this.selectedParams).forEach(key => {
  //       const paramConfig = this.selectedParams[key];
  //       const paramType = paramConfig[2];
  //       const defaultValue = paramType === 'CheckBox' ? false : '';
  //       formControls[key] = [defaultValue];
  //     });
  //   }

  //   this.dynamicForm = this.fb.group(formControls);
  // }

  // Method to start the spinner and begin timing
  public startSpinner(): void {
    this.showSpinner = true;
    this.spinnerStartTime = Date.now();
    this.spinnerEndTime = null;
    console.log('Spinner started at:', new Date(this.spinnerStartTime).toISOString());
  }

  // // Method to stop the spinner and calculate duration
  // public stopSpinner(): void {
  //   if (this.spinnerStartTime) {
  //     this.spinnerEndTime = Date.now();
  //     this.spinnerDuration = this.spinnerEndTime - this.spinnerStartTime;
  //     this.totalSpinnerTime += this.spinnerDuration;

  //     console.log('Spinner stopped at:', new Date(this.spinnerEndTime).toISOString());
  //     console.log('Spinner was active for:', this.getFormattedDuration(this.spinnerDuration));
  //     console.log('Total spinner time in session:', this.getFormattedDuration(this.totalSpinnerTime));
  //   }

  //   this.showSpinner = false;
  // }

  // Modified stopSpinner method with additional safety
  public stopSpinner(): void {
    if (this.showSpinner && this.spinnerStartTime) {
      this.spinnerEndTime = Date.now();
      this.spinnerDuration = this.spinnerEndTime - this.spinnerStartTime;
      this.totalSpinnerTime += this.spinnerDuration;

      console.log('Spinner stopped at:', new Date(this.spinnerEndTime).toISOString());
      console.log('Spinner was active for:', this.getFormattedDuration(this.spinnerDuration));
      console.log('Total spinner time in session:', this.getFormattedDuration(this.totalSpinnerTime));
    } else {
      console.warn('Spinner stop called but spinner was not active');
    }

    this.showSpinner = false;

    // Clear any pending timeouts
    if (this.downloadTimeoutId) {
      clearTimeout(this.downloadTimeoutId);
      this.downloadTimeoutId = null;
    }
  }

  // Helper method to format duration in a readable format
  public getFormattedDuration(milliseconds: number): string {
    if (milliseconds < 1000) {
      return `${milliseconds}ms`;
    } else if (milliseconds < 60000) {
      return `${(milliseconds / 1000).toFixed(2)}s`;
    } else {
      const minutes = Math.floor(milliseconds / 60000);
      const seconds = ((milliseconds % 60000) / 1000).toFixed(2);
      return `${minutes}m ${seconds}s`;
    }
  }

  // Public method to get current spinner duration (if active)
  public getCurrentSpinnerDuration(): number {
    if (this.showSpinner && this.spinnerStartTime) {
      return Date.now() - this.spinnerStartTime;
    }
    return this.spinnerDuration;
  }

  // Public method to get formatted current duration
  public getFormattedCurrentDuration(): string {
    return this.getFormattedDuration(this.getCurrentSpinnerDuration());
  }

  // Helper methods for template date formatting
  public getFormattedStartTime(): string {
    return this.spinnerStartTime ? new Date(this.spinnerStartTime).toLocaleTimeString() : '';
  }

  public getFormattedEndTime(): string {
    return this.spinnerEndTime ? new Date(this.spinnerEndTime).toLocaleTimeString() : '';
  }

  public getFormattedStartDateTime(): string {
    return this.spinnerStartTime ? new Date(this.spinnerStartTime).toLocaleString() : '';
  }

  public getFormattedEndDateTime(): string {
    return this.spinnerEndTime ? new Date(this.spinnerEndTime).toLocaleString() : '';
  }

  // Optional: Method to reset total spinner time (call when needed)
  public resetTotalSpinnerTime(): void {
    this.totalSpinnerTime = 0;
    console.log('Total spinner time reset');
  }

  // Optional: Method to get spinner statistics
  public getSpinnerStats(): { lastDuration: number, totalDuration: number, isActive: boolean } {
    return {
      lastDuration: this.spinnerDuration,
      totalDuration: this.totalSpinnerTime,
      isActive: this.showSpinner
    };
  }

  // NEW METHOD: Reset connection data when operation changes
  private resetConnectionData() {
    // Reset connection objects
    this.sourceConnection = undefined as any;
    this.targetConnection = undefined as any;

    // Reset file upload states
    this.isTxtFileUploadedSource = false;
    this.isTxtFileUploadedTarget = false;
    this.selectedSourceFile = undefined;
    this.selectedTargetFile = undefined;

    // Reset headers
    this.sourceHeaders = [];
    this.targetHeaders = [];
    this.selectedHeaders = {}; // 🔁 Reset selected headers
    this.isDropdownOpen = {};  // 🔁 Reset dropdown state

    // Reset UI states
    this.isPerformBtnVisible = false;
    this.isTestParametersVisible = false;
    this.isSourceQueryVisible = false;
    this.isTargetQueryVisible = false;
    this.isReportVisible = false;

    // Reset result data
    this.resultHeader = undefined;
    this.result = undefined;
    this.resultReports = null;
    this.resultHeaderReports = '';
    this.showReports = false;

    // Reset queries
    this.sourceQuery = undefined;
    this.targetQuery = undefined;

    this.selectedKeyColumns = [];
    this.showSpinner = false;

    this.paramEntriesResult = [];
    this.showModal = false;
    this.selectedTable = null;
    this.queryType = 'source';
    this.sourceFormReady = false;
    this.targetFormReady = false;
    this.showButtons = false;

    // Reset button text
    this.connectButtonText = 'Connect';

    // Reset child component forms if they exist
    if (this.sourceConnectionForm) {
      this.sourceConnectionForm.reset();
    }
    if (this.targetConnectionForm) {
      this.targetConnectionForm.reset();
    }

    // Trigger change detection to update UI
    this.cdRef.detectChanges();
  }


  // Helper method to get parameter entries for template
  getParamEntries() {
    console.log('param entries', this.paramEntriesResult)
    if (!this.paramEntriesResult.length) {
      if (!this.selectedParams) return [];
      this.paramEntriesResult = Object.entries(this.selectedParams).map(([key, value]) => ({
        key,
        label: (value as string[])[0],
        type: (value as string[])[2],
        description: (value as string[])[1] || ''
      }));
      return this.paramEntriesResult;
    }
    return this.paramEntriesResult;
  }



  // Method to get dropdown options based on parameter key
  getDropdownOptions(paramKey: string): string[] {
    switch (paramKey) {
      case 'header':
      case 'column_to_test':
      case 'primary_key_source':
      case 'compare_columns_source': // Add this new case
        return this.sourceConnection?.headers || [];

      case 'primary_key_target':
      case 'compare_columns_target': // Add this new case
        return this.targetConnection?.headers || [];

      case 'data_types':
        return this.sourceConnection?.dataTypes || [];

      default:
        return [];
    }
  }

  // Updated createTest method to include dynamic parameters
  // createTest(): Test {
  //   const testParams: Record<string, any> = {
  //     on_index: true,
  //   };

  //   // Add dynamic form values to test_params
  //   if (this.dynamicForm && this.selectedParams) {
  //     Object.keys(this.selectedParams).forEach(key => {
  //       const value = this.dynamicForm.get(key)?.value;
  //       if (value !== null && value !== undefined && value !== '') {
  //         testParams[key] = value;
  //       }
  //     });
  //   }

  //   return {
  //     operation: this.connectionForm.value.operation || this.dynamicForm?.get('operation')?.value,
  //     logged_in_user_id: 0,
  //     test_params: testParams,
  //     test_type: this.selectedFunction?.funct_type
  //   };
  // }

  createTest(): Test {
    const testParams: Record<string, any> = {
      on_index: true,
    };

    // Add dynamic form values to test_params
    if (this.dynamicForm && this.selectedParams) {
      Object.keys(this.selectedParams).forEach(key => {
        const value = this.dynamicForm.get(key)?.value;
        if (value !== null && value !== undefined && value !== '') {
          // Handle array values (from multi-select)
          if (Array.isArray(value)) {
            testParams[key] = value;
          } else if (typeof value === 'string' && value.includes(',')) {
            // Handle comma-separated values
            testParams[key] = value.split(',').map(v => v.trim());
          } else {
            testParams[key] = value;
          }
        }
      });
    }

    return {
      operation: this.connectionForm.value.operation || this.dynamicForm?.get('operation')?.value,
      logged_in_user_id: 0,
      test_params: testParams,
      test_type: this.selectedFunction?.funct_type
    };
  }

  // onConnect() {
  //   const button = document.querySelector(`#btn_connect`) as HTMLButtonElement;
  //   this.sourceConnection = this.sourceConnectionForm.createConnection();
  //   if(this.showTargetConnection){
  //     this.targetConnection = this.targetConnectionForm.createConnection()
  //   }
  //   const test = this.createTest();

  //   const params = {
  //     Test: test,
  //     Connection: {
  //       source: this.sourceConnection,
  //       ...(this.targetConnection ? { target: this.targetConnection } : {})
  //     }
  //   };
  //   this.resultHeader = 'Connecting...';
  //   button.classList.add('connecting');
  //   this.apiService.connect(params).subscribe({
  //     next: (data) => {
  //       button.classList.remove('connecting');
  //       this.connectButtonText = 'Connected';

  //       this.resultHeader = data.connection_status;
  //       this.isPerformBtnVisible = true;
  //       this.isTestParametersVisible = true;
  //       // this.isSourceQueryVisible = true;

  //       this.sourceConnection.headers = data.source_headers;
  //       if (this.targetConnection && data.target_headers) {
  //         this.targetConnection.headers = data.target_headers;
  //         //this.isTargetQueryVisible = true;
  //       }
  //     },
  //     error: (error) => console.error('Error:', error)
  //   });
  // }

  // onConnect() {
  //   const button = document.querySelector(`#btn_connect`) as HTMLButtonElement;

  //   this.sourceConnection = this.sourceConnectionForm.createConnection();
  //   if (this.showTargetConnection) {
  //     this.targetConnection = this.targetConnectionForm.createConnection();
  //   }

  //   const test = this.createTest();
  //   const params = {
  //     Test: test,
  //     Connection: {
  //       source: this.sourceConnection,
  //       ...(this.targetConnection ? { target: this.targetConnection } : {})
  //     }
  //   };

  //   this.resultHeader = 'Connecting...';
  //   this.showSpinner = true;
  //   button.classList.add('connecting');

  //   this.apiService.connect(params).subscribe({
  //     next: (data) => {
  //       this.showSpinner = false;
  //       button.classList.remove('connecting');
  //       this.connectButtonText = 'Connected';
  //       this.isConnectBtnVisible = false;

  //       this.resultHeader = data.connection_status;
  //       this.isPerformBtnVisible = true;
  //       this.isTestParametersVisible = true;

  //       this.sourceConnection.headers = data.source_headers;
  //       this.sourceConnection.dataTypes = data.source_datatypes;
  //       if (this.targetConnection && data.target_headers) {
  //         this.targetConnection.headers = data.target_headers;
  //         this.targetConnection.dataTypes = data.target_datatypes;
  //       }
  //     },
  //     error: (error) => {
  //       console.error('Error:', error);
  //       this.showSpinner = false;
  //       button.classList.remove('connecting');
  //     }
  //   });
  // }

  // Updated onConnect method with spinner timing
  onConnect() {
    const button = document.querySelector(`#btn_connect`) as HTMLButtonElement;

    this.sourceConnection = this.sourceConnectionForm.createConnection();
    if (this.showTargetConnection) {
      this.targetConnection = this.targetConnectionForm.createConnection();
    }

    const test = this.createTest();
    const params = {
      Test: test,
      Connection: {
        source: this.sourceConnection,
        ...(this.targetConnection ? { target: this.targetConnection } : {})
      }
    };

    this.resultHeader = 'Connecting...';
    this.startSpinner(); // Start spinner timing
    button.classList.add('connecting');

    this.apiService.connect(params).subscribe({
      next: (data) => {
        this.stopSpinner(); // Stop spinner timing
        button.classList.remove('connecting');
        this.connectButtonText = 'Connected';
        this.isConnectBtnVisible = false;

        this.resultHeader = data.connection_status;
        this.isPerformBtnVisible = true;
        this.isTestParametersVisible = true;

        this.sourceConnection.headers = data.source_headers;
        this.sourceConnection.dataTypes = data.source_datatypes;
        if (this.targetConnection && data.target_headers) {
          this.targetConnection.headers = data.target_headers;
          this.targetConnection.dataTypes = data.target_datatypes;
        }

        // Optional: Show success message with duration
        this.toastr.success(
          `Connection established in ${this.getFormattedDuration(this.spinnerDuration)}`,
          'Success'
        );
      },
      error: (error) => {
        console.error('Error:', error);
        this.stopSpinner(); // Stop spinner timing even on error
        button.classList.remove('connecting');

        // Optional: Show error message with duration
        this.toastr.error(
          `Connection failed after ${this.getFormattedDuration(this.spinnerDuration)}`,
          'Error'
        );
      }
    });
  }

  onSubmit() {
    this.onConnect();
  }

  // showSuccess() {
  //   this.toastr.success('Operation successful!', 'Success');
  // }

  // onPerform() {
  //   const sourceConnection = this.sourceConnection;
  //   const targetConnection = this.targetConnection;
  //   const test = this.createTest();

  //   const params = {
  //     Test: test,
  //     Connection: {
  //     source: sourceConnection,
  //     ...(targetConnection && { target: targetConnection })
  //     }
  //   };

  //   this.resultHeader = 'In Progress';
  //   this.resultReports = null;
  //   this.apiService.performValidation(params).subscribe({
  //     next: (data) => {
  //       // this.resultHeader = data.result;
  //       // this.result = data.console_output;
  //       //trigger reports after result is not null
  //       this.resultHeaderReports = data.result || 'Result Received';
  //       console.log('resultHeaderReports:' + this.resultHeaderReports);

  //       this.resultReports = data.console_output || data;
  //       console.log('resultReports:' + this.resultReports);

  //       this.resultHeader = 'Fetched Report Data';
  //       this.isReportVisible = true;
  //     },
  //     error: (error) => {
  //       console.error('Error:', error);
  //       this.resultHeader = 'Error during validation';
  //       this.result = error.message || 'Unknown error';
  //     }
  //   });
  // }

  // onPerform() {
  //   const sourceConnection = this.sourceConnection;
  //   const targetConnection = this.targetConnection;
  //   const test = this.createTest();

  //   // ✅ Step 0: Warn if "check_duplicate" test and no key columns selected
  //   // if (
  //   //   test?.operation === 'check_duplicate' &&
  //   //   (!this.selectedKeyColumns || this.selectedKeyColumns.length === 0)

  //   // ) {
  //   //   console.log('Selected Columns:', this.selectedKeyColumns);
  //   //   this.toastr.warning(
  //   //     'Please select one or more key columns before performing the Duplicate Check test.',
  //   //     'Missing Input'
  //   //   );
  //   //   return;
  //   // }

  //   // ✅ Step 1: Validate required inputs before API call
  //   if (!sourceConnection || !test) {
  //     this.resultHeader = 'Missing Required Inputs';
  //     this.result = 'Source connection or test configuration is missing.';
  //     this.toastr.error('Please provide all required inputs', 'Validation Error');
  //     return;
  //   }

  //   this.showSpinner = true;

  //   const params = {
  //     Test: test,
  //     Connection: {
  //       source: sourceConnection,
  //       ...(targetConnection && { target: targetConnection })
  //     }
  //   };

  //   this.resultHeader = 'In Progress';
  //   this.resultReports = null;
  //   this.isReportVisible = false;

  //   this.apiService.performValidation(params).subscribe({
  //     next: (data) => {
  //       this.resultHeaderReports = data.result || 'Result Received';
  //       this.resultReports = data.console_output || data;

  //       this.resultHeader = 'Fetched Report Data';
  //       this.isReportVisible = true;
  //     },
  //     error: (error) => {
  //       console.error('Error:', error);

  //       if (error.status === 0) {
  //         this.result = 'Network error or server not reachable.';
  //       } else if (error.status === 400) {
  //         this.result = 'Bad request. Please check input values.';
  //       } else if (error.status === 500) {
  //         this.result = 'Internal server error. Please try again later.';
  //       } else {
  //         this.result = error.message || 'Unexpected error occurred.';
  //       }

  //       this.resultHeader = 'Error during validation';
  //       this.isReportVisible = false;

  //       this.toastr.error(this.result, 'Validation Failed');
  //     }
  //   }).add(() => {
  //     this.showSpinner = false;
  //   });
  // }
  // Update the onPerform method with validation
  // onPerform() {
  //   const sourceConnection = this.sourceConnection;
  //   const targetConnection = this.targetConnection;
  //   const test = this.createTest();

  //   // ✅ Step 1: Validate required key columns for specific operations
  //   if (this.isKeyColumnRequired() && !this.hasSelectedKeyColumns()) {
  //     const operationName = this.getOperationDisplayName();
  //     this.toastr.warning(
  //       `Please select one or more key columns before performing the ${operationName} test.`,
  //       'Missing Required Input'
  //     );
  //     return;
  //   }

  //   // ✅ Step 2: General validation for required inputs
  //   if (!sourceConnection || !test) {
  //     this.resultHeader = 'Missing Required Inputs';
  //     this.result = 'Source connection or test configuration is missing.';
  //     this.toastr.error('Please provide all required inputs', 'Validation Error');
  //     return;
  //   }

  //   this.showSpinner = true;

  //   const params = {
  //     Test: test,
  //     Connection: {
  //       source: sourceConnection,
  //       ...(targetConnection && { target: targetConnection })
  //     }
  //   };

  //   this.resultHeader = 'In Progress';
  //   this.resultReports = null;
  //   this.isReportVisible = false;

  //   this.apiService.performValidation(params).subscribe({
  //     next: (data) => {
  //       this.resultHeaderReports = data.result || 'Result Received';
  //       this.resultReports = data.console_output || data;

  //       this.resultHeader = 'Fetched Report Data';
  //       this.isReportVisible = true;
  //     },
  //     error: (error) => {
  //       console.error('Error:', error);

  //       if (error.status === 0) {
  //         this.result = 'Network error or server not reachable.';
  //       } else if (error.status === 400) {
  //         this.result = 'Bad request. Please check input values.';
  //       } else if (error.status === 500) {
  //         this.result = 'Internal server error. Please try again later.';
  //       } else {
  //         this.result = error.message || 'Unexpected error occurred.';
  //       }

  //       this.resultHeader = 'Error during validation';
  //       this.isReportVisible = false;

  //       this.toastr.error(this.result, 'Validation Failed');
  //     }
  //   }).add(() => {
  //     this.showSpinner = false;
  //   });
  // }

  // Updated onPerform method with spinner timing
  onPerform() {
    const sourceConnection = this.sourceConnection;
    const targetConnection = this.targetConnection;
    const test = this.createTest();

    // Validation logic (existing code)
    if (this.isKeyColumnRequired() && !this.hasSelectedKeyColumns()) {
      const operationName = this.getOperationDisplayName();
      this.toastr.warning(
        `Please select one or more key columns before performing the ${operationName} test.`,
        'Missing Required Input'
      );
      return;
    }

    if (!sourceConnection || !test) {
      this.resultHeader = 'Missing Required Inputs';
      this.result = 'Source connection or test configuration is missing.';
      this.toastr.error('Please provide all required inputs', 'Validation Error');
      return;
    }

    this.startSpinner(); // Start spinner timing

    const params = {
      Test: test,
      Connection: {
        source: sourceConnection,
        ...(targetConnection && { target: targetConnection })
      }
    };

    this.resultHeader = 'In Progress';
    this.resultReports = null;
    this.isReportVisible = false;

    this.apiService.performValidation(params).subscribe({
      next: (data) => {
        this.stopSpinner(); // Stop spinner timing

        this.resultHeaderReports = data.result || 'Result Received';
        this.resultReports = data.console_output || data;

        this.resultHeader = 'Fetched Report Data';
        this.isReportVisible = true;

        // Optional: Show success message with duration
        this.toastr.success(
          `Test completed in ${this.getFormattedDuration(this.spinnerDuration)}`,
          'Success'
        );
      },
      error: (error) => {
        console.error('Error:', error);
        this.stopSpinner(); // Stop spinner timing even on error

        if (error.status === 0) {
          this.result = 'Network error or server not reachable.';
        } else if (error.status === 400) {
          this.result = 'Bad request. Please check input values.';
        } else if (error.status === 500) {
          this.result = 'Internal server error. Please try again later.';
        } else {
          this.result = error.message || 'Unexpected error occurred.';
        }

        this.resultHeader = 'Error during validation';
        this.isReportVisible = false;

        this.toastr.error(
          `${this.result} (Duration: ${this.getFormattedDuration(this.spinnerDuration)})`,
          'Validation Failed'
        );
      }
    });
  }

  // Helper method to check if current operation requires key columns
  private isKeyColumnRequired(): boolean {
    const currentOperation = this.connectionForm.get('operation')?.value ||
      this.dynamicForm?.get('operation')?.value;
    return this.operationsRequiringKeyColumns.includes(currentOperation);
  }

  // Helper method to check if key columns are selected
  public hasSelectedKeyColumns(): boolean {
    const keyColumnParams = ['header', 'column_to_test', 'primary_key_source', 'compare_columns_source',
      'compare_columns_target'];

    return keyColumnParams.some(paramKey => {
      const selectedHeaders = this.selectedHeaders[paramKey];
      return selectedHeaders && selectedHeaders.length > 0;
    });
  }



  // Helper method to get display name for operation
  private getOperationDisplayName(): string {
    const currentOperation = this.connectionForm.get('operation')?.value ||
      this.dynamicForm?.get('operation')?.value;

    const operationNames: { [key: string]: string } = {
      'check_duplicate': 'Duplicate Check',
      'data_type_check': 'Data Type Check',
      'data_compare': 'Data Comparison',
      'data_comparison': 'Data Comparison'
    };

    return operationNames[currentOperation] || 'Selected';
  }


  // Update the getSelectedHeadersText method to show validation state
  getSelectedHeadersText(paramKey: string): string {
    const selected = this.selectedHeaders[paramKey] || [];
    const requiredParams = ['header', 'column_to_test', 'primary_key_source', 'primary_key_target', 'compare_columns_source', 'compare_columns_target'];
    const isRequired = this.isKeyColumnRequired() && requiredParams.includes(paramKey);

    if (selected.length === 0) {
      return isRequired ? 'Select Columns (Required)' : 'Select Columns';
    } else if (selected.length === 1) {
      return selected[0];
    } else {
      return `${selected.length} column selected`;
    }
  }

  // Optional: Add visual indicator for required fields
  isFieldRequired(paramKey: string): boolean {
    const requiredFields = [
      'header',
      'column_to_test',
      'primary_key_source',
      'primary_key_target',
      'compare_columns_source', // Add if this should be required
      'compare_columns_target'  // Add if this should be required
    ];

    return this.isKeyColumnRequired() && requiredFields.includes(paramKey);
  }

  onReset() {
    localStorage.clear();

    this.isPerformBtnVisible = false;
    this.isTestParametersVisible = false;
    this.isConnectBtnVisible = false;
    this.isSourceQueryVisible = false;
    this.isTargetQueryVisible = false;
    this.isReportVisible = false;
    this.resetButton = false;
    this.resultHeader = '';
    console.log(this.resultHeader);

    this.result = '';
    this.resultHeaderReports = '';
    this.resultReports = null;
    this.showReports = false;

    this.connectButtonText = 'Connect';
    this.selectedTable = null;
    this.selectedSourceFile = undefined;
    this.selectedTargetFile = undefined;
    this.sourceHeaders = [];
    this.targetHeaders = [];
    this.showTargetConnection = false;

    this.selectedParams = null;
    this.selectedFunction = null;
    this.sourceQuery = undefined;
    this.targetQuery = undefined;
    this.sourceConnection = undefined as any;
    this.targetConnection = undefined as any;

    this.selectedHeaders = {}; // 🔁 Reset selected headers
    this.isDropdownOpen = {};  // 🔁 Reset dropdown state

    // Reset spinner timing data
    this.spinnerStartTime = null;
    this.spinnerEndTime = null;
    this.spinnerDuration = 0;
    this.totalSpinnerTime = 0; // Now resets total time as well
    this.showSpinner = false;

    this.connectionForm.reset({
      operation: ''
    });

    this.dynamicForm = this.fb.group({});

    // Optionally reset child components if needed
    if (this.sourceConnectionForm) this.sourceConnectionForm.reset();
    if (this.targetConnectionForm) this.targetConnectionForm.reset();

    this.cdRef.detectChanges();
  }

  openQueryBuilder(type: 'source' | 'target') {
    console.log("QUERY BUILDER")
    this.queryType = type;
    this.showModal = true;
    setTimeout(() => {
      if (this.queryBuilder) {
        this.queryBuilder.openModal();
      }
    }, 100);
  }

  closeModal() {
    this.showModal = false;
    if (this.queryBuilder) {
      this.queryBuilder.closeModal();
    }
    this.selectedTable = null;
    if (this.sourceQuery || this.targetQuery) {
      this.connectButtonText = 'Re-Load With Query?';
    }
  }

  onQueryGenerated(query: string) {
    if (this.queryType === 'source') {
      this.sourceQuery = query;
    } else if (this.queryType === 'target') {
      this.targetQuery = query;
    }
    this.closeModal();
  }

  loadFunctionDefinitions() {
    this.apiService.run_query("EXEC GetAllFunctionalities;").subscribe({
      next: (data: any[]) => {
        this.functionList = data.map(item => ({
          ...item,
          funct_params: item.funct_params ? JSON.parse(item.funct_params) : {}
        }));
      },
      error: (err) => console.error('Failed to load function definitions', err)
    });
  }

  onSourceConnectionCreated(conn: Connection) {
    this.sourceConnection = conn;
  }

  onTargetConnectionCreated(conn: Connection) {
    this.targetConnection = conn;
  }


  // Updated methods for dynamic header selection
  toggleHeaderDropdown(paramKey: string) {
    this.isDropdownOpen[paramKey] = !this.isDropdownOpen[paramKey];
  }

  onHeaderCheckboxChange(paramKey: string, header: string, event: Event) {
    event.stopPropagation();
    const checkbox = event.target as HTMLInputElement;

    if (!this.selectedHeaders[paramKey]) {
      this.selectedHeaders[paramKey] = [];
    }

    if (checkbox.checked) {
      console.log('selectedParams', this.selectedParams[paramKey], this.selectedParams[paramKey].includes('MultiSelect'));

      if (!this.selectedHeaders[paramKey].includes(header)) {
        if (this.selectedParams[paramKey].includes('MultiSelect')) {
          this.selectedHeaders[paramKey].push(header);
        } else {
          this.selectedHeaders[paramKey] = [header];
        }
      }
    } else {
      this.selectedHeaders[paramKey] = this.selectedHeaders[paramKey].filter(h => h !== header);
    }

    // Update the dynamic form control
    this.updateFormControl(paramKey);


  }

  //   saveScrollPosition(paramKey: string, scrollTop: number): void {
  //     this.scrollPositions[paramKey] = scrollTop;
  //   }

  //   restoreScrollPosition(paramKey: string): void {
  //     const dropdownEl = this.dropdownMenus.find(el => el.nativeElement.getAttribute('data-param-key') === paramKey);
  //     if (dropdownEl && this.scrollPositions[paramKey] != null) {
  //       dropdownEl.nativeElement.scrollTop = this.scrollPositions[paramKey];
  //     }
  //   }

  trackByOption(index: number, option: string): string {
    return option;
  }

  // onHeaderCheckboxChange(paramKey: string, header: string, event: Event) {
  //   const checkbox = event.target as HTMLInputElement;

  //   if (!this.selectedHeaders[paramKey]) {
  //     this.selectedHeaders[paramKey] = [];
  //   }

  //   // Save current scroll
  //   this.saveScrollPosition(paramKey, this.scrollPositions[paramKey] || 0);

  //   if (checkbox.checked) {
  //     if (!this.selectedHeaders[paramKey].includes(header)) {
  //       if (this.selectedParams[paramKey].includes('MultiSelect')) {
  //         this.selectedHeaders[paramKey].push(header);
  //       } else {
  //         this.selectedHeaders[paramKey] = [header];
  //       }
  //     }
  //   } else {
  //     this.selectedHeaders[paramKey] = this.selectedHeaders[paramKey].filter(h => h !== header);
  //   }

  //   this.updateFormControl(paramKey);

  //   // Restore scroll position after DOM update
  //   setTimeout(() => this.restoreScrollPosition(paramKey), 0);
  // }



  // Helper to check if all options are selected
  areAllOptionsSelected(paramKey: string): boolean {
    const allOptions = this.getDropdownOptions(paramKey);
    const selected = this.selectedHeaders[paramKey] || [];
    return allOptions.length > 0 && allOptions.every(option => selected.includes(option));
  }

  // Toggle all selections
  onSelectAllHeaders(paramKey: string, event: any): void {
    event?.stopPropagation();
    const isChecked = event.target.checked;
    const allOptions = this.getDropdownOptions(paramKey);

    if (isChecked) {
      this.selectedHeaders[paramKey] = [...allOptions];
    } else {
      this.selectedHeaders[paramKey] = [];
    }
    this.updateFormControl(paramKey);
    // Trigger form control value change if needed
    // this.dynamicForm.get(paramKey)?.setValue(this.selectedHeaders[paramKey]);
  }

  isHeaderSelected(paramKey: string, header: string): boolean {
    return this.selectedHeaders[paramKey]?.includes(header) || false;
  }

  // getSelectedHeadersText(paramKey: string): string {
  //   const selected = this.selectedHeaders[paramKey] || [];
  //   if (selected.length === 0) {
  //     return 'Select key Columns';
  //   } else if (selected.length === 1) {
  //     return selected[0];
  //   } else {
  //     return `${selected.length} items selected`;
  //   }
  // }

  // Check if parameter should use multi-select dropdown
  isMultiSelectParam(paramKey: string): boolean {
    const multiSelectParams = ['header', 'column_to_test', 'primary_key_source', 'primary_key_target', 'compare_columns_source',
      'compare_columns_target'];
    return multiSelectParams.includes(paramKey);
  }

  // Update form control with selected values
  updateFormControl(paramKey: string) {
    let selectedValues = this.selectedHeaders[paramKey];

    // Normalize to array if it's a single string
    if (typeof selectedValues === 'string') {
      selectedValues = [selectedValues];
    }

    // Default to empty array if undefined/null
    selectedValues = selectedValues || [];

    const formControl = this.dynamicForm.get(paramKey);

    if (formControl) {

      console.log('this.selectedParams', this.selectedParams[paramKey].includes('MultiSelect'))

      // For single select, use the first value; for multi-select, join with comma
      const value = this.selectedParams[paramKey].includes('MultiSelect') ? selectedValues : selectedValues.join(',');
      formControl.setValue(value);
    }
  }

  // Close dropdown when clicking outside
  @HostListener('document:click', ['$event'])
  onDocumentClick(event: Event) {
    const target = event.target as HTMLElement;
    if (!target.closest('.custom-dropdown')) {
      // Close all dropdowns
      Object.keys(this.isDropdownOpen).forEach(key => {
        this.isDropdownOpen[key] = false;
      });
    }
  }

  // Add these methods to your PerformComponent class

  onSourceFileUploadCompleted(isCompleted: boolean) {
    if (isCompleted) {
      console.log('Source file upload completed');

      // Make connect button and reset button visible
      // this.isConnectBtnVisible = true;
      // this.resetButton = true;

      // Optionally update other UI states
      this.isTxtFileUploadedSource = true;

      // Check if buttons should be visible based on operation type
      this.updateButtonVisibility();

      // Trigger change detection
      this.cdRef.detectChanges();
    }
  }

  onTargetFileUploadCompleted(isCompleted: boolean) {
    if (isCompleted) {
      console.log('Target file upload completed');

      // Make connect button and reset button visible
      this.isConnectBtnVisible = true;
      this.resetButton = true;

      // Optionally update other UI states
      this.isTxtFileUploadedTarget = true;

      // Check if buttons should be visible based on operation type
      this.updateButtonVisibility();

      // Trigger change detection
      this.cdRef.detectChanges();
    }
  }

  // Optional: Method to handle when both files are uploaded (for compare operations)
  checkBothFilesUploaded() {
    if (this.showTargetConnection) {
      // For compare operations, both files should be uploaded
      return this.isTxtFileUploadedSource && this.isTxtFileUploadedTarget;
    } else {
      // For single connection operations, only source file needed
      return this.isTxtFileUploadedSource;
    }
  }
  onSourceFormReady(isReady: boolean) {
    this.sourceFormReady = isReady;
    this.updateButtonVisibility();
  }

  onTargetFormReady(isReady: boolean) {
    this.targetFormReady = isReady;
    this.updateButtonVisibility();
  }

  updateButtonVisibility() {
    if (this.showTargetConnection) {
      this.showButtons = this.sourceFormReady && this.targetFormReady;
      this.isConnectBtnVisible = this.sourceFormReady && this.targetFormReady;
    } else {
      this.showButtons = this.sourceFormReady;
      this.isConnectBtnVisible = this.sourceFormReady;
    }
  }

  // Prevent checkbox click from bubbling up
  onCheckboxClick(event: Event) {
    event.stopPropagation(); // Prevents the dropdown from closing or losing focus
  }

  // Handle individual checkbox changes with focus preservation
  onHeaderCheckboxChangeFixed(event: Event, paramKey: string, option: string, index: number) {
    event.stopPropagation();

    // Your existing logic for handling checkbox change
    this.onHeaderCheckboxChange(paramKey, option, event);

    // Keep focus on the current item to prevent jumping
    const currentElement = event.target as HTMLElement;
    setTimeout(() => {
      if (currentElement && currentElement.closest('.dropdown-item-custom')) {
        const parentItem = currentElement.closest('.dropdown-item-custom') as HTMLElement;
        parentItem.focus();
      }
    }, 0);
  }

  // Handle dropdown item clicks with focus preservation
  onDropdownItemClick(event: Event, paramKey: string, option: string, index: number) {
    event.preventDefault();
    event.stopPropagation();

    // Toggle the checkbox without changing focus
    const isCurrentlySelected = this.isHeaderSelected(paramKey, option);

    // Create a mock event for the checkbox change
    const mockEvent = {
      target: { checked: !isCurrentlySelected }
    } as any;

    this.onHeaderCheckboxChange(paramKey, option, mockEvent);

    // Keep focus on the current item
    const currentElement = event.currentTarget as HTMLElement;
    setTimeout(() => {
      currentElement.focus();
    }, 0);
  }

  // Handle Select All checkbox click
  onSelectAllCheckboxClick(event: Event) {
    event.stopPropagation();
  }

  // Handle Select All changes with focus preservation
  onSelectAllHeadersChange(event: Event, paramKey: string) {
    event.stopPropagation();

    // Your existing Select All logic
    this.onSelectAllHeaders(paramKey, event);

    // Keep focus on the Select All item
    const currentElement = event.target as HTMLElement;
    setTimeout(() => {
      if (currentElement && currentElement.closest('.dropdown-item-custom')) {
        const parentItem = currentElement.closest('.dropdown-item-custom') as HTMLElement;
        parentItem.focus();
      }
    }, 0);
  }

  // Handle Select All item click
  onSelectAllItemClick(event: Event, paramKey: string) {
    event.preventDefault();
    event.stopPropagation();

    // Toggle all options
    const allSelected = this.areAllOptionsSelected(paramKey);
    const mockEvent = {
      target: { checked: !allSelected }
    } as any;

    this.onSelectAllHeaders(paramKey, mockEvent);

    // Keep focus on the current item
    const currentElement = event.currentTarget as HTMLElement;
    setTimeout(() => {
      currentElement.focus();
    }, 0);
  }
// Helper method to set execution status
private setExecutionStatus(
  status: 'connecting' | 'connected' | 'error' | 'in-progress' | 'downloading' | 'downloaded'
) {
  switch (status) {
    case 'connecting':
      this.resultHeader = 'Connecting...';
      break;
    case 'connected':
      this.resultHeader = 'Connected';
      break;
    case 'error':
      this.resultHeader = 'Connection Error';
      break;
    case 'in-progress':
      this.resultHeader = 'In Progress';
      break;
    case 'downloading':
      this.resultHeader = 'Downloading file...';
      break;
    case 'downloaded':
      this.resultHeader = 'File Downloaded';
      break;
  }
  this.cdRef.detectChanges();
}


onDownloadStart() {
console.log('📥 Download started - starting spinner');
this.startSpinner(); // Start spinner timing

// Set execution status to connecting
this.setExecutionStatus('downloading');

// Set a timeout as fallback to stop spinner
this.downloadTimeoutId = setTimeout(() => {
  console.warn('⚠️ Download timeout reached, forcing spinner stop');
  this.stopSpinner();
  this.setExecutionStatus('error');
}, this.DOWNLOAD_TIMEOUT_MS);
}

onDownloadFinish() {
console.log('✅ Download finished - stopping spinner');

// Clear the timeout since download completed normally
if (this.downloadTimeoutId) {
  clearTimeout(this.downloadTimeoutId);
  this.downloadTimeoutId = null;
}

this.stopSpinner(); // Stop spinner timing

// Update execution status to show as connected
this.setExecutionStatus('downloaded');
}

  getSelectedHeaders(paramKey: string): string[] {
    return this.selectedHeaders?.[paramKey] || [];
  }
  // Method to update button visibility based on file upload status
  // updateButtonVisibility() {
  //   const shouldShowButtons = this.checkBothFilesUploaded();
  //   this.isConnectBtnVisible = shouldShowButtons;
  //   this.resetButton = shouldShowButtons;

  //   console.log('Button visibility updated:', {
  //     showTargetConnection: this.showTargetConnection,
  //     sourceUploaded: this.isTxtFileUploadedSource,
  //     targetUploaded: this.isTxtFileUploadedTarget,
  //     shouldShowButtons: shouldShowButtons
  //   });
  // }

}
