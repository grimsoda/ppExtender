import { render, screen } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { vi } from 'vitest';
import App from './App';

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      retry: false,
    },
  },
});

vi.mock('./api-hooks', () => ({
  useHealth: () => ({
    isLoading: false,
    isError: false,
    isSuccess: true,
    data: { status: 'ok', database: 'connected' },
  }),
  useGetCohort: () => ({
    isLoading: false,
    data: null,
    error: null,
  }),
  useGetBeatmapPlays: () => ({
    isLoading: false,
    data: null,
    error: null,
  }),
  useGetRecommendations: () => ({
    isLoading: false,
    data: [],
    error: null,
  }),
}));

describe('App', () => {
  it('renders the title', () => {
    render(
      <QueryClientProvider client={queryClient}>
        <App />
      </QueryClientProvider>
    );
    expect(screen.getByText('osu! Recommender')).toBeInTheDocument();
  });
});
