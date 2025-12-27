import { CardinalityContainer } from '@/container/CardinalityContainer';
import { CardinalityProvider } from '@/providers/Cardinality';
import { ThemeToggle } from '@/components/ThemeToggle';

export function CardinalityPage() {
  return (
    <CardinalityProvider>
      <div className="min-h-screen bg-background">
        <div className="container mx-auto py-8 px-4">
          {/* Header */}
          <div className="mb-8 flex items-start justify-between">
            <div>
              <h1 className="text-3xl font-bold tracking-tight">Cardinality Explorer</h1>
              <p className="mt-2 text-muted-foreground">
                Analyze Prometheus metric cardinality to identify high-cardinality metrics and track
                their growth over time.
              </p>
            </div>
            <ThemeToggle />
          </div>

          {/* Main Content */}
          <CardinalityContainer />
        </div>
      </div>
    </CardinalityProvider>
  );
}
