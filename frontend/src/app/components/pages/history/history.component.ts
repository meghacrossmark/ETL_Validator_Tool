import { CommonModule } from '@angular/common';
import { Component, OnInit} from '@angular/core';
import { FormsModule } from '@angular/forms';

interface TestRecord {
  id: number;
  testName: string;
  confirmedBy: string;
  status: 'Failed' | 'Passed';
}

@Component({
  selector: 'app-history',
  imports: [FormsModule, CommonModule],
  templateUrl: './history.component.html',
  styleUrl: './history.component.scss'
})

export class HistoryComponent implements OnInit {
// Sample test records
    originalTestRecords: TestRecord[] = [
      {
        id: 1,
        testName: 'Performance Test',
        confirmedBy: 'John Doe',
        status: 'Failed'
      },
      {
        id: 2,
        testName: 'Security Audit',
        confirmedBy: 'Jane Smith',
        status: 'Passed'
      },
      {
        id: 3,
        testName: 'Usability Test',
        confirmedBy: 'Mike Johnson',
        status: 'Passed'
      }
    ];
  
    // Filtered test records
    filteredTestRecords: TestRecord[] = [];
  
    // Single filter input
    filterInput = '';
  
    ngOnInit() {
      // Initialize filtered records with all records
      this.filteredTestRecords = [...this.originalTestRecords];
    }
  
    // Method to apply filters
    applyFilters() {
      if (!this.filterInput) {
        // If no filter input, show all records
        this.filteredTestRecords = [...this.originalTestRecords];
        return;
      }
  
      // Convert filter input to lowercase for case-insensitive search
      const searchTerm = this.filterInput.toLowerCase();
  
      this.filteredTestRecords = this.originalTestRecords.filter(record => {
        // Check if the search term matches any of the fields
        return (
          record.testName.toLowerCase().includes(searchTerm) ||
          record.confirmedBy.toLowerCase().includes(searchTerm) ||
          record.status.toLowerCase().includes(searchTerm) 
        );
      });
    }
  
    // Helper method to format date for searching
    private formatDate(date: Date): string {
      return date.toLocaleDateString('en-US', {
        year: 'numeric',
        month: '2-digit',
        day: '2-digit'
      }).toLowerCase();
    }
  
    // Method to view report (placeholder)
    viewReport(record: TestRecord) {
      // Implement report generation logic
      console.log('Generating report for:', record);
      alert(`Generating report for test: ${record.testName}`);
    }
  }