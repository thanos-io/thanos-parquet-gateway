import { AlertCircle, RefreshCw } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Card, CardContent } from '@/components/ui/card';
import { APIError } from '@/types/api';

interface ErrorStateProps {
  error: Error;
  onRetry?: () => void;
}

export function ErrorState({ error, onRetry }: ErrorStateProps) {
  const isAPIError = error instanceof APIError;

  return (
    <Card className="border-destructive/50">
      <CardContent className="flex flex-col items-center justify-center py-12">
        <AlertCircle className="h-12 w-12 text-destructive" />
        <h3 className="mt-4 text-lg font-semibold">Failed to load data</h3>
        <p className="mt-2 text-center text-sm text-muted-foreground">
          {isAPIError ? (
            <>
              <span className="font-mono text-xs">{error.code}</span>
              <br />
              {error.message}
            </>
          ) : (
            error.message
          )}
        </p>
        {onRetry && (
          <Button variant="outline" className="mt-4" onClick={onRetry}>
            <RefreshCw className="mr-2 h-4 w-4" />
            Try again
          </Button>
        )}
      </CardContent>
    </Card>
  );
}
