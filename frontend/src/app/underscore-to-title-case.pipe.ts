import { Pipe, PipeTransform } from '@angular/core';

@Pipe({
  name: 'underscoreToTitleCase'
})
export class UnderscoreToTitleCasePipe implements PipeTransform {
  transform(value: string): string {
    if (!value) return value;

    // Split by underscores, capitalize each word, move 'Check' to the end
    const words = value.split('_').map(word => 
      word.charAt(0).toUpperCase() + word.slice(1).toLowerCase()
    );

    const checkIndex = words.findIndex(w => w.toLowerCase() === 'check');
    if (checkIndex !== -1) {
      const [checkWord] = words.splice(checkIndex, 1);
      words.push(checkWord); // move "Check" to end
    }

    return words.join(' ');
  }
}
