import { Component, Input, OnInit, SimpleChanges } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { FormArray, FormBuilder, FormGroup, ReactiveFormsModule } from '@angular/forms';
import { CommonModule } from '@angular/common';

@Component({
  selector: 'app-query-builder',
  standalone: true,
  imports: [ReactiveFormsModule,CommonModule],
  templateUrl: './query-builder.component.html',
  styleUrls: ['./query-builder.component.scss']
})
export class QueryBuilderComponent implements OnInit {
  @Input() columns: { name: string, type: string }[] = [];
  @Input() queryType: 'source' | 'target' = 'source';
  queryForm: FormGroup;
  generatedQuery: string = '';
  queryResult: any[] = [];
  isModalOpen: boolean = false;

  sqlFunctions: string[] = [
    'cast', 'coalesce', 'CONCAT', 'current_timestamp', 'DATE_FORMAT',
    'nvl', 'replace', 'round', 'row_number', 'split', 'string',
    'substring', 'sum', 'TO_DATE', 'TO_TIMESTAMP', 'trim', 'try_cast', 'uuid'
  ];

  sqlConditions: string[] = [
    '=', '!=', '>', '<', '>=', '<=', 'LIKE', 'IN', 'BETWEEN', 'IS NULL', 'IS NOT NULL'
  ];

  constructor(private fb: FormBuilder, private http: HttpClient) {
    this.queryForm = this.fb.group({
      fields: this.fb.array([]),
      conditions: this.fb.array([])
    });
  }

  ngOnInit() {
    this.initializeForm();
  }

  ngOnChanges(changes: SimpleChanges) {
    if (changes['columns'] && !changes['columns'].firstChange) {
      console.log("QueryBuilder ngOnChanges - columns changed:", this.columns);
      this.initializeForm();
    }
  }

  initializeForm() {
    console.log("Loading start BUILDER - columns:", this.columns);

    const fieldsArray = this.queryForm.get('fields') as FormArray;
    while (fieldsArray.length !== 0) {
      fieldsArray.removeAt(0);
    }

    const conditionsArray = this.queryForm.get('conditions') as FormArray;
    while (conditionsArray.length !== 0) {
      conditionsArray.removeAt(0);
    }

    if (this.columns && this.columns.length > 0) {
      this.columns.forEach(column => {
        fieldsArray.push(this.fb.group({
          name: [column.name],
          isSelected: [false],
          applyFunction: [false],
          selectedFunction: [''],
          functionParams: ['']
        }));
      });
    } else {
      console.warn("No columns provided to QueryBuilder");
    }
    
    console.log("Loading Close - fields array length:", fieldsArray.length);
  }
  get fields() {
    return this.queryForm.get('fields') as FormArray;
  }

  get conditions() {
    return this.queryForm.get('conditions') as FormArray;
  }

  addCondition() {
    this.conditions.push(this.fb.group({
      column: [''],
      condition: [''],
      value: ['']
    }));
  }

  removeCondition(index: number) {
    this.conditions.removeAt(index);
  }

  generateQuery() {
    let selectClause = '';
    const selectedFields = this.fields.controls
      .filter(field => field.get('isSelected')?.value)
      .map(field => {
        const name = field.get('name')?.value;
        if (field.get('applyFunction')?.value) {
          const func = field.get('selectedFunction')?.value;
          const params = field.get('functionParams')?.value || '';
          return `${func}(${name}${params ? `, ${params}` : ''}) AS ${name}`;
        }
        return name;
      });

    selectClause = selectedFields.length > 0 ? selectedFields.join(', ') : '*';
    let whereClause = '';

    if (this.conditions.length > 0) {
      const conditions = this.conditions.controls.map(condition => {
        const column = condition.get('column')?.value;
        const cond = condition.get('condition')?.value;
        const value = condition.get('value')?.value;
        return `${column} ${cond} ${value}`;
      });
      whereClause = `WHERE ${conditions.join(' AND ')}`;
    }

    this.generatedQuery = `SELECT ${selectClause} FROM ${this.queryType.toLowerCase()}_table ${whereClause};`;
  }

  testQuery() {
    this.http.post('/api/execute-query', { query: this.generatedQuery })
      .subscribe({
        next: (response: any) => {
          this.queryResult = response.data.slice(0, 20); // Limit to 20 rows
        },
        error: (error) => {
          console.error('Query execution failed:', error);
          this.queryResult = [];
        }
      });
  }

  openModal() {
    this.isModalOpen = true;
    this.initializeForm();
  }

  closeModal() {
    this.isModalOpen = false;
    this.generatedQuery = '';
    this.queryResult = [];
  }
}