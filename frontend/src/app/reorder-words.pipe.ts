import { Pipe, PipeTransform } from '@angular/core';

@Pipe({
  name: 'reorderWords'
})
export class ReorderWordsPipe implements PipeTransform {
  transform(value: string): string {
    if (!value) return value;

    const words = value.trim().split(' ');
    if (words.length === 2) {
      return `${words[1]} ${words[0]}`;
    }

    return value; // If not exactly 2 words, return as-is
  }
}
