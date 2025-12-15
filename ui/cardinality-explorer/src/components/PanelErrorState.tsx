import { AlertCircle, RefreshCw } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { APIError } from '@/types/api';

interface PanelErrorStateProps {
  title: string;
  error: Error;
  onRetry?: () => void;
}

/**
 * Error state component for individual panels.
 * Shows error within a card without affecting other panels on the page.
 */
export function PanelErrorState({ title, error, onRetry }: PanelErrorStateProps) {
  const isAPIError = error instanceof APIError;

  return (
    <Card className="border-destructive/50">
      <CardHeader className="pb-2">
        <CardTitle className="text-base font-medium">{title}</CardTitle>
      </CardHeader>
      <CardContent className="flex flex-col items-center justify-center py-8">
        <AlertCircle className="h-8 w-8 text-destructive" />
        <p className="mt-3 text-center text-sm text-muted-foreground">
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
          <Button variant="outline" size="sm" className="mt-3" onClick={onRetry}>
            <RefreshCw className="mr-2 h-3 w-3" />
            Retry
          </Button>
        )}
      </CardContent>
    </Card>
  );
}
