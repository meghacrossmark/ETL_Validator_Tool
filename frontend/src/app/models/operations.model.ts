export interface FunctionDefinition {
    id: number;
    funct_name: string;
    funct_value: string;
    funct_type: string;
    funct_params: { [key: string]: [label: string, formControlName: string, inputType: string] };
  }
  