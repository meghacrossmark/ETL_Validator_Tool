import { Component } from '@angular/core';
import { RouterOutlet } from '@angular/router';
import { PerformComponent } from "./components/pages/perform/perform.component";

@Component({
  selector: 'app-root',
  imports: [RouterOutlet],
  templateUrl: './app.component.html',
  styleUrl: './app.component.scss'
})
export class AppComponent {
  title = 'Validator';
}
