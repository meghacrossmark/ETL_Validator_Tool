import { CommonModule } from '@angular/common';
import { HttpClient } from '@angular/common/http';
import { Component, EventEmitter, Input, Output } from '@angular/core';
import { FormatLabelPipe } from "../../../format-label.pipe";
import { ReorderWordsPipe } from "../../../reorder-words.pipe";
import { UnderscoreToTitleCasePipe } from "../../../underscore-to-title-case.pipe";
import { CleanSuffixPipe } from "../../../clean-suffix.pipe";
import * as XLSX from 'xlsx-js-style';
import { saveAs } from 'file-saver';
import { FormsModule } from '@angular/forms';

@Component({
  selector: 'app-report',
  imports: [CommonModule, FormatLabelPipe, ReorderWordsPipe, UnderscoreToTitleCasePipe, CleanSuffixPipe, FormsModule],
  templateUrl: './report.component.html',
  styleUrl: './report.component.scss',
  standalone: true
})
export class ReportComponent {
  @Input() resultHeader: string = '';
  @Input() show: boolean = false;
  @Input() spinnerDuration!: number;
  @Input() totalSpinnerTime!: number;
  @Input() formatFn!: (ms: number) => string;

  // Provide default reportData if not passed from parent
  @Input() reportData: any = {
    result_metadata: {
      columns: [
        { column: 'ID', unexpected_index_list: [] },
        { column: 'Name', unexpected_index_list: [2] },
        { column: 'Age', unexpected_index_list: [1, 4] },
        { column: 'Email', unexpected_index_list: [1, 3] }
      ]
    }
  };
  @Output() downloadStarted = new EventEmitter<void>();
  @Output() downloadFinished = new EventEmitter<void>();

  showButtonSaveQuery = false;
  duplicateCount: number = 0;
  duplicateValueCounts: { [key: string]: number } = {};
  mismatchedRows: { [key: string]: string }[] = [];
  missingInSourceGrouped: { [key: string]: string[] } = {};
  missingInTargetGrouped: { [key: string]: string[] } = {};
  missingInSourceFormatted: any[] = [];
  missingInTargetFormatted: any[] = [];
  tableHeaders: string[] = [];
  mismatchedFilterdData: any = []
  isDownloading = false;
  // selectedFormat: '' | 'excel' | 'csv' = '';
  selectedFormat: string = '';
  expandedValues = new Set<string>();

  ngOnInit() {
    if (this.reportData) {
      this.processComparisonResult(this.reportData);
    }
    console.log('Report Data:', this.reportData);
    console.log('Error Field:', this.reportData?.error);
  }

  private flattenObject(obj: any, parentKey = '', result: any = {}): any {
    for (const key in obj) {
      if (obj.hasOwnProperty(key)) {
        const newKey = parentKey ? `${parentKey}.${key}` : key;
        if (typeof obj[key] === 'object' && obj[key] !== null) {
          this.flattenObject(obj[key], newKey, result);
        } else {
          result[newKey] = obj[key];
        }
      }
    }
    return result;
  }

  downloadReport() {
    if (!this.selectedFormat) {
      alert('Please select a file format before exporting.');
      return;
    }
  
    console.log('🚀 Starting download...');
    this.downloadStarted.emit(); // notify parent spinner start
  
    // Add a small delay to ensure spinner starts
    setTimeout(() => {
      try {
        if (this.selectedFormat === 'excel') {
          console.log('📊 Generating Excel file...');
          const wb: XLSX.WorkBook = XLSX.utils.book_new();
  
          const createSheet = (rows: any[], sheetName: string) => {
            if (!rows || rows.length === 0) {
              const ws = XLSX.utils.aoa_to_sheet([
                [{ v: 'No data', s: { font: { italic: true } } }]
              ]);
              XLSX.utils.book_append_sheet(wb, ws, sheetName);
              return;
            }
  
            let columns = Object.keys(rows[0]);
            if (columns.includes("Id")) {
              columns = ["Id", ...columns.filter(c => c !== "Id")];
            }
  
            const data = [
              columns.map(col => ({ v: col, s: { font: { bold: true } } })),
              ...rows.map(row => columns.map(col => row[col] ?? ''))
            ];
  
            const ws = XLSX.utils.aoa_to_sheet(data);
            ws['!cols'] = columns.map(c => ({ wch: Math.max(c.length, 12) }));
            XLSX.utils.book_append_sheet(wb, ws, sheetName);
          };
  
          createSheet(this.reportData?.failed_rows_details?.not_in_source_rows, 'not_in_source_rows');
          createSheet(this.reportData?.failed_rows_details?.not_in_target_rows, 'not_in_target_rows');
  
          XLSX.writeFile(wb, 'DataCompareReport.xlsx');
          console.log('✅ Excel file generated successfully');
        } 
        
        else if (this.selectedFormat === 'csv') {
          console.log('📄 Generating CSV files...');
          const createCSV = (rows: any[], fileName: string) => {
            if (!rows || rows.length === 0) {
              const blob = new Blob(['No data'], { type: 'text/csv;charset=utf-8;' });
              saveAs(blob, fileName);
              return;
            }
  
            let columns = Object.keys(rows[0]);
            if (columns.includes("Id")) {
              columns = ["Id", ...columns.filter(c => c !== "Id")];
            }
  
            const csvRows = [];
            csvRows.push(columns.join(','));
            rows.forEach(row => {
              csvRows.push(columns.map(col => JSON.stringify(row[col] ?? '')).join(','));
            });
  
            const blob = new Blob([csvRows.join('\n')], { type: 'text/csv;charset=utf-8;' });
            saveAs(blob, fileName);
          };
  
          createCSV(this.reportData?.failed_rows_details?.not_in_source_rows, 'not_in_source_rows.csv');
          createCSV(this.reportData?.failed_rows_details?.not_in_target_rows, 'not_in_target_rows.csv');
          console.log('✅ CSV files generated successfully');
        }
  
        // Add another delay before finishing to ensure file download completes
        setTimeout(() => {
          console.log('🏁 Download finished, stopping spinner');
          this.downloadFinished.emit(); // notify parent spinner stop
        }, 1000); // 1 second delay
  
      } catch (error) {
        console.error('❌ Error during download:', error);
        // Always emit downloadFinished even on error
        this.downloadFinished.emit();
      }
    }, 100); // 100ms delay to ensure spinner starts
  }

  get headersWithNullCount(): number {
    const columns = this.reportData?.result_metadata?.columns || [];
    return columns.filter(
      (col: { unexpected_index_list: string | any[] }) =>
        col.unexpected_index_list?.length > 0
    ).length;
  }

  get nullHeaderInfo(): { label: string; values?: string[]; joinedValues?: string } {
    const columns = this.reportData?.result_metadata?.columns || [];
    const failedHeaders = columns
      .filter(
        (col: { unexpected_index_list: string | any[] }) =>
          col.unexpected_index_list?.length > 0
      )
      .map((col: { column: any }) => col.column);

    return failedHeaders.length
      ? {
        label: 'Null Containing Column Headers:',
        values: failedHeaders,
        joinedValues: failedHeaders.join(', ')
      }
      : {
        label: 'Null Containing Column Headers:',
        joinedValues: ''
      };
  }

  getSelectedColumns(): string[] {
    return this.reportData?.Test?.test_params?.column_to_test || [];
  }

  getNullCounts(): { column: string; nullCount: number }[] {
    const columns = this.reportData?.result_metadata?.columns || [];
    return columns.map((col: { column: any; unexpected_index_list: string | any[]; }) => ({
      column: col.column,
      nullCount: col.unexpected_index_list?.length || 0
    }));
  }

    get duplicateKeys(): string[] {
    return Object.keys(this.duplicateValueCounts || {});
  }

  generateDuplicateTableHtml(reportData: any): string {
    const results = reportData?.result_metadata?.results || [];
    const selectedColumns: string[] = reportData?.Test?.test_params?.column_to_test || [];

    if (!results.length || !selectedColumns.length) {
      return `<div class="fst-italic text-muted ps-3">No duplicate data found for the selected combination.</div>`;
    }

    let hasDuplicates = false;

    if (selectedColumns.length === 1) {
      const col = selectedColumns[0];
      const result = results.find((r: any) => r.column === col);
      if (result?.duplicate_value_details && Object.keys(result.duplicate_value_details).length > 0) {
        hasDuplicates = true;
      }
    } else {
      const combinedResult = results.find((r: any) => r.row_duplicate_details);
      if (combinedResult?.row_duplicate_details && Object.keys(combinedResult.row_duplicate_details).length > 0) {
        hasDuplicates = true;
      }
    }

    if (!hasDuplicates) {
      return `<div class="fst-italic text-muted ps-3">No duplicate data found for the selected combination.</div>`;
    }

    // Start table
    let html = `<div class="mt-3 mb-2"><strong>Duplicate Values Summary</strong></div>`;
    html += `<table class="table table-bordered table-striped"><thead class="table-light"><tr>`;

    // Table Header
    for (const col of selectedColumns) {
      html += `<th>${col}</th>`;
    }
    html += `<th>Duplicate Count</th></tr></thead><tbody>`;

    if (selectedColumns.length === 1) {
      const col = selectedColumns[0];
      const result = results.find((r: any) => r.column === col);
      const duplicates = result?.duplicate_value_details || {};

      for (const [value, detail] of Object.entries(duplicates)) {
        html += `<tr><td>${value}</td><td>${(detail as any).count}</td></tr>`;
      }
    } else {
      const combinedResult = results.find((r: any) => r.row_duplicate_details);
      const rowDuplicates = combinedResult?.row_duplicate_details || {};

      for (const [key, detail] of Object.entries(rowDuplicates)) {
        const values: string[] = [];

        // Parse combined key: ('Sunil', 39, 'abc@example.com')
        const cleaned = key.slice(1, -1);
        const regex = /'([^']*)'|(\d+)/g;
        let match;
        while ((match = regex.exec(cleaned)) !== null) {
          values.push(match[1] || match[2]);
        }

        html += `<tr>`;
        for (let i = 0; i < selectedColumns.length; i++) {
          html += `<td>${values[i] || ""}</td>`;
        }
        html += `<td>${(detail as any).count}</td></tr>`;
      }
    }

    html += `</tbody></table>`;
    return html;
  }

  // Extracts all names with duplicates (keys of duplicate_value_details)
  getDuplicateValueKeys(result: any): string[] {
    return result?.duplicate_value_details ? Object.keys(result.duplicate_value_details) : [];
  }

  // Formats index numbers to row format like R1, R5, etc.
  formatIndexes(indexes: number[]): string {
    return indexes?.map(i => `R${i + 2}`).join(', ');
  }

  getDuplicateColumnCount(reportData: any): number {
    if (!reportData?.result_metadata?.results) {
      return 0;
    }

    return reportData.result_metadata.results.filter((result: any) =>
      result.duplicate_value_details && Object.keys(result.duplicate_value_details).length > 0
    ).length;
  }

  getTotalElementCount(reportData: any): number {
    if (!reportData?.result_metadata?.results?.length) return 0;

    // Return element_count from the first column result
    return reportData.result_metadata.results[0].element_count || 0;
  }

  getColumnsWithDuplicates(reportData: any): string[] {
    const results = reportData?.result_metadata?.results || [];
    return results
      .filter((res: any) =>
        res.duplicate_value_details && Object.keys(res.duplicate_value_details).length > 0
      )
      .map((res: any) => res.column);
  }

  // total count of records from result_metadata
  get totalRecordsCount(): number {
    return this.reportData?.result_metadata?.results?.[0]?.total_count || 0;
  }

  // count of records that match the expected data type
  get matchDataTypeCount(): number {
    return this.reportData?.result_metadata?.results?.[0]?.match_count || 0;
  }

  // count of records that do NOT match the expected data type
  get notMatchingDataTypeCount(): number {
    return this.reportData?.result_metadata?.results?.[0]?.not_match_count || 0;
  }

  // count of null values in the tested column
  get nullCount(): number {
    return this.reportData?.result_metadata?.results?.[0]?.null_count || 0;
  }

  get testedColumnName(): string {
    return this.reportData?.Test?.test_params?.column_to_test || '';
  }

  get nonMatchingValueCounts(): { value: string; count: number; indexes: number[] }[] {
    return this.reportData?.result_metadata?.results?.[0]?.non_matching_unique_values_with_counts || [];
  }
  
  getIndexesToShow(item: { value: string; indexes?: number[] }): number[] {
  const indexes = item.indexes ?? [];
  return this.isExpanded(item) ? indexes : indexes.slice(0, 10);
}
  
  toggleIndexes(item: { value: string }) {
    if (this.isExpanded(item)) {
      this.expandedValues.delete(item.value);
    } else {
      this.expandedValues.add(item.value);
    }
  }
  
  isExpanded(item: { value: string }): boolean {
    return this.expandedValues.has(item.value);
  }
  
  normalizeType(type: string): string {
    const lowerType = type?.toLowerCase() ?? '';

    if (lowerType.includes('datetime64')) return 'timestamp';
    if (lowerType === 'object') return 'string';
    if (lowerType === 'int64') return 'int';

    return lowerType;
  }

  getTypeMismatchRows(): { expected: string, actual: string }[] {
    const results = this.reportData?.result_metadata?.results || [];

    return results.map((item: any) => ({
      expected: this.normalizeType(item.expected_type),
      actual: this.normalizeType(item.actual_type)
    }));
  }

 // Simple getter that directly checks and formats the error
 get errorMessage(): string | null {
  const rawError = this.reportData?.error || this.reportData?.Error || null;

  if (!rawError) return null;

  // 🔍 Debug full object in console
  console.log("❌ Full reportData object with error:", this.reportData);

  const errorStr = String(rawError);

  // Column mismatch case
  if (errorStr.includes("compare_columns_source and compare_columns_target must have the same length")) {
    const sourceColumns = this.reportData.Test?.test_params?.compare_columns_source?.length || 0;
    const targetColumns = this.reportData.Test?.test_params?.compare_columns_target?.length || 0;

    const sourceFile = this.reportData.Connection?.source?.conn_params?.selected_fileName || "source file";
    const targetFile = this.reportData.Connection?.target?.conn_params?.selected_fileName || "target file";

    return `Column selection mismatch! Source data has ${sourceColumns} columns selected, but Target data has ${targetColumns} columns selected. Please select the same number of columns for comparison.`;
  }

  // Fallback
  return `Raw error: ${errorStr}`;
}



// Simple boolean getter to check if error exists
get hasError(): boolean {
  return !!(this.reportData?.error);
}

  processComparisonResult(result: any): void {
    const failedDetails = result?.failed_rows_details?.mismatched_rows || []; // Add support for future mismatches
    const notInSourceList = result?.failed_rows_details?.not_in_source_rows || [];
    const notInTargetList = result?.failed_rows_details?.not_in_target_rows || [];
  
    // 1. Store mismatched rows if needed
    this.mismatchedRows = failedDetails.map((item: any) => {
      const row: { [key: string]: string } = {};
      Object.keys(item).forEach(key => {
        row[key] = item[key];
      });
      return row;
    });
  
    // 2. Convert mismatched rows to table format
    this.mismatchedFilterdData = this.jsonToTable(
      failedDetails,
      result?.Test?.test_params?.primary_key_source ?? [],
      result?.Test?.test_params?.compare_columns_source ?? []
    );
    // console.log('failedDetails', this.mismatchedFilterdData);
  
    // 3. Group not-in-source and not-in-target by header (if needed elsewhere)
    this.missingInSourceGrouped = this.groupByHeader(notInSourceList);
    this.missingInTargetGrouped = this.groupByHeader(notInTargetList);
  
    // 4. Collect headers dynamically (excluding index/primary_key if present)
    const allKeys = [
      ...notInSourceList.flatMap((item: any) => Object.keys(item)),
      ...notInTargetList.flatMap((item: any) => Object.keys(item))
    ];
    
    const uniqueKeys = Array.from(new Set(allKeys)).filter(
      key => key !== 'index' && key !== 'primary_key'
    );
    
    // Move 'Id' to the front if present
    const idIndex = uniqueKeys.indexOf('Id');
    if (idIndex > -1) {
      uniqueKeys.splice(idIndex, 1); // remove 'Id'
      uniqueKeys.unshift('Id'); // add 'Id' to the beginning
    }
    
    this.tableHeaders = uniqueKeys;
    
    // 5. Store for rendering
    this.missingInSourceFormatted = notInSourceList;
    this.missingInTargetFormatted = notInTargetList;
  }
  

  groupByHeader(list: any[]): { [key: string]: string[] } {
    const grouped: { [key: string]: string[] } = {};
    list.forEach(item => {
      Object.keys(item).forEach(key => {
        if (key !== 'index' && key !== 'primary_key') {
          if (!grouped[key]) {
            grouped[key] = [];
          }
          grouped[key].push(item[key]);
        }
      });
    });
    return grouped;
  }

  // To support Object.keys in template
  objectKeys = Object.keys;

  public jsonToTable(data: any[], primaryKeys: string[] = [], compareColumns: string[] = []): { header: string[], rows: string[][] } {
    const allColumns = new Set<string>();
  
    // 1. Collect all base column names (remove _source/_target suffixes)
    data.forEach(entry => {
      Object.keys(entry).forEach(key => {
        if (key.endsWith('_source')) {
          allColumns.add(key.replace('_source', ''));
        } else if (key.endsWith('_target')) {
          allColumns.add(key.replace('_target', ''));
        } else {
          allColumns.add(key);
        }
      });
    });
  
    // 2. Define column order: primary keys → compare columns → others
    const uniqueColumns = Array.from(allColumns);
    const otherColumns = uniqueColumns.filter(col =>
      !primaryKeys.includes(col) && !compareColumns.includes(col)
    );
  
    const finalColumns = [...primaryKeys, ...compareColumns, ...otherColumns];
  
    // 3. Prepare header row
    const header = ['DataSource', ...finalColumns];
  
    // 4. Prepare row data
    const rows: string[][] = [];
  
    data.forEach(entry => {
      const sourceRow = ['Source'];
      const targetRow = ['Target'];
  
      finalColumns.forEach(col => {
        sourceRow.push(String(entry[`${col}_source`] ?? entry[col] ?? ''));
        targetRow.push(String(entry[`${col}_target`] ?? entry[col] ?? ''));
      });
  
      rows.push(sourceRow, targetRow);
    });
  
    return { header, rows };
  }
  
  // Add this method to your ReportComponent class

/**
 * TrackBy function for Angular *ngFor to improve performance
 * when rendering large datasets
 */
trackByIndex(index: number, item: any): number {
  return index;
}

/**
 * Get the maximum number of columns that can be displayed
 * before triggering horizontal scroll
 */
getMaxVisibleColumns(): number {
  // Calculate based on screen width and minimum column width
  const screenWidth = window.innerWidth;
  const minColumnWidth = 120; // matches CSS min-width
  const containerPadding = 100; // account for padding and margins
  
  return Math.floor((screenWidth - containerPadding) / minColumnWidth);
}

/**
 * Check if horizontal scrolling will be needed
 */
willRequireHorizontalScroll(): boolean {
  return this.tableHeaders.length > this.getMaxVisibleColumns();
}

/**
 * Get table dimensions info for display
 */
getTableInfo(): { rows: number; columns: number; willScroll: boolean } {
  const totalRows = Math.max(
    this.missingInSourceFormatted.length,
    this.missingInTargetFormatted.length
  );
  
  return {
    rows: totalRows,
    columns: this.tableHeaders.length,
    willScroll: this.willRequireHorizontalScroll()
  };
}

/**
 * Export table data (can be enhanced later)
 */
exportTableData(data: any[], filename: string = 'table_data.csv'): void {
  if (!data.length || !this.tableHeaders.length) {
    console.warn('No data to export');
    return;
  }

  const csvContent = [
    // Header row
    this.tableHeaders.join(','),
    // Data rows
    ...data.map(row => 
      this.tableHeaders.map(header => 
        `"${(row[header] || '').toString().replace(/"/g, '""')}"`
      ).join(',')
    )
  ].join('\n');

  const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
  const link = document.createElement('a');
  
  if (link.download !== undefined) {
    const url = URL.createObjectURL(blob);
    link.setAttribute('href', url);
    link.setAttribute('download', filename);
    link.style.visibility = 'hidden';
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
  }
}

/**
 * Utility method to truncate long text for display
 */
truncateText(text: string, maxLength: number = 50): string {
  if (!text) return '';
  return text.length > maxLength ? text.substring(0, maxLength) + '...' : text;
}

}
