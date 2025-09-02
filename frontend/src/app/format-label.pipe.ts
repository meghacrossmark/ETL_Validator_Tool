import { Pipe, PipeTransform } from '@angular/core';

@Pipe({
  name: 'formatLabel'
})
export class FormatLabelPipe implements PipeTransform {
  transform(value: string): string {
    if (!value) return '';
    return value
      .replace(/_/g, ' ') // Replace underscores with spaces
      .split(' ')
      .map(word => word.charAt(0).toUpperCase() + word.slice(1)) // Capitalize each word
      .join(' ');
  }
}
