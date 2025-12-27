import { createBrowserRouter, Navigate } from 'react-router-dom';
import { CardinalityPage } from '@/pages/CardinalityPage';
import { MetricDetailPage } from '@/pages/MetricDetailPage';
import { WelcomePage } from '@/pages/WelcomePage';

export const router = createBrowserRouter(
  [
    {
      path: '/',
      element: <WelcomePage />,
    },
    {
      path: '/cardinality',
      element: <CardinalityPage />,
    },
    {
      path: '/cardinality/metric/:metricName',
      element: <MetricDetailPage />,
    },
    {
      path: '*',
      element: <Navigate to="/" replace />,
    },
  ],
  {
    basename: '/ui',
  }
);
