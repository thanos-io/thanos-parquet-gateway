import { RouterProvider } from 'react-router-dom';
import { ThemeProvider } from './providers/Theme';
import { QueryProvider } from './providers/Query';
import { router } from './router';

function App() {
  return (
    <ThemeProvider defaultTheme="dark" storageKey="vite-ui-theme">
      <QueryProvider>
        <RouterProvider router={router} />
      </QueryProvider>
    </ThemeProvider>
  );
}

export default App;
