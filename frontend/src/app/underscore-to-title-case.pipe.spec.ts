import { UnderscoreToTitleCasePipe } from './underscore-to-title-case.pipe';

describe('UnderscoreToTitleCasePipe', () => {
  it('create an instance', () => {
    const pipe = new UnderscoreToTitleCasePipe();
    expect(pipe).toBeTruthy();
  });
});
