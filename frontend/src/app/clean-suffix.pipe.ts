import { Pipe, PipeTransform } from '@angular/core';

@Pipe({
  name: 'cleanSuffix'
})
export class CleanSuffixPipe implements PipeTransform {
  transform(value: string): string {
    if (!value) return value;

    // Remove trailing _0, _1, _123, etc.
    return value.replace(/_\d+$/, '').trim();
  }
}
