import { App } from "./App";

// Airflow's ReactPlugin loader expects a component at globalThis.AirflowPlugin
(globalThis as Record<string, unknown>).AirflowPlugin = App;

export default App;
