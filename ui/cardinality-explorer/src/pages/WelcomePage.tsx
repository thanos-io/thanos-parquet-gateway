import { Link } from 'react-router-dom';
import { BarChart3, Activity, TrendingUp } from 'lucide-react';
import { Card, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { ThemeToggle } from '@/components/ThemeToggle';

export function WelcomePage() {
  return (
    <div className="min-h-screen bg-background">
      {/* Theme Toggle - Top Right */}
      <div className="absolute top-4 right-4">
        <ThemeToggle />
      </div>

      <div className="container mx-auto px-4 py-16">
        {/* Header */}
        <div className="text-center mb-12">
          <h1 className="text-4xl font-bold tracking-tight mb-4">
            Thanos Parquet Gateway
          </h1>
          <p className="text-xl text-muted-foreground max-w-2xl mx-auto">
            Analyze and explore your metrics cardinality.
          </p>
        </div>

        {/* Feature Cards */}
        <div className="grid gap-6 md:grid-cols-3 max-w-4xl mx-auto mb-12">
          <Card>
            <CardHeader>
              <div className="rounded-lg bg-blue-500/10 w-10 h-10 flex items-center justify-center mb-2">
                <BarChart3 className="h-5 w-5 text-blue-500" />
              </div>
              <CardTitle className="text-lg">Cardinality Analysis</CardTitle>
              <CardDescription>
                Discover which metrics consume the most series and track cardinality over time.
              </CardDescription>
            </CardHeader>
          </Card>

          <Card>
            <CardHeader>
              <div className="rounded-lg bg-green-500/10 w-10 h-10 flex items-center justify-center mb-2">
                <Activity className="h-5 w-5 text-green-500" />
              </div>
              <CardTitle className="text-lg">Time Series Visualization</CardTitle>
              <CardDescription>
                Interactive charts showing how metric cardinality changes across days.
              </CardDescription>
            </CardHeader>
          </Card>

          <Card>
            <CardHeader>
              <div className="rounded-lg bg-purple-500/10 w-10 h-10 flex items-center justify-center mb-2">
                <TrendingUp className="h-5 w-5 text-purple-500" />
              </div>
              <CardTitle className="text-lg">Metric History</CardTitle>
              <CardDescription>
                Deep dive into individual metrics to understand trends and anomalies.
              </CardDescription>
            </CardHeader>
          </Card>
        </div>

        {/* CTA */}
        <div className="text-center">
          <Link to="/cardinality">
            <Button size="lg" className="px-8">
              <BarChart3 className="mr-2 h-5 w-5" />
              Explore Cardinality
            </Button>
          </Link>
        </div>

      </div>
    </div>
  );
}
