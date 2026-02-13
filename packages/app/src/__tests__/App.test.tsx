import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, waitFor } from '@testing-library/react';
import '@testing-library/jest-dom';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import App from '../App';
import * as apiHooks from '../api-hooks';

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      retry: false,
    },
  },
});

function renderWithQueryClient(ui: React.ReactElement) {
  return render(<QueryClientProvider client={queryClient}>{ui}</QueryClientProvider>);
}

vi.mock('../api-hooks', async () => {
  const actual = await vi.importActual<typeof import('../api-hooks')>('../api-hooks');
  return {
    ...actual,
    useHealth: vi.fn(() => ({
      isLoading: false,
      isError: false,
      isSuccess: true,
      data: { status: 'ok', database: 'connected' },
    })),
    useGetCohort: vi.fn(),
    useGetBeatmapPlays: vi.fn(),
    useGetRecommendations: vi.fn(),
  };
});

const { useGetCohort, useGetBeatmapPlays, useGetRecommendations } = apiHooks as any;

describe('App Integration', () => {
  const mockCohortData = {
    size: 150,
    ppDistribution: { min: 180, max: 450, mean: 320, median: 310 },
    accuracyDistribution: { min: 85.5, max: 99.8, mean: 94.2, median: 95.1 },
    topPlayers: [
      { userId: 1, username: 'Player1', pp: 450, accuracy: 99.5 },
    ],
    plays: [],
    seedPpRange: { lower: 200, upper: 500 },
  };

  const mockRecommendations = [
    {
      beatmapId: 101,
      title: 'Test Beatmap 1',
      artist: 'Test Artist 1',
      difficulty: 'Hard',
      stars: 4.5,
      pp: 320,
      accuracy: 96.5,
      mods: ['HD'],
      coverUrl: 'https://example.com/cover1.jpg',
    },
  ];

  beforeEach(() => {
    vi.clearAllMocks();
    (useGetCohort as any).mockReturnValue({
      isLoading: false,
      data: null,
      error: null,
    });
    (useGetBeatmapPlays as any).mockReturnValue({
      isLoading: false,
      data: null,
      error: null,
    });
    (useGetRecommendations as any).mockReturnValue({
      isLoading: false,
      data: null,
      error: null,
    });
  });

  it('should render app title', () => {
    renderWithQueryClient(<App />);

    expect(screen.getByText('osu! Recommender')).toBeInTheDocument();
  });

  it('should render SeedInput component', () => {
    renderWithQueryClient(<App />);

    expect(screen.getByLabelText(/beatmap id/i)).toBeInTheDocument();
  });

  it('should display cohort preview after successful fetch', async () => {
    (useGetCohort as any).mockReturnValue({
      isLoading: false,
      data: mockCohortData,
      error: null,
    });

    renderWithQueryClient(<App />);

    await waitFor(() => {
      expect(screen.getByText(/cohort size/i)).toBeInTheDocument();
      expect(screen.getByText('150')).toBeInTheDocument();
    });
  });

  it('should display recommendations after fetch', async () => {
    (useGetCohort as any).mockReturnValue({
      isLoading: false,
      data: mockCohortData,
      error: null,
    });
    (useGetRecommendations as any).mockReturnValue({
      isLoading: false,
      data: mockRecommendations,
      error: null,
    });

    renderWithQueryClient(<App />);

    await waitFor(() => {
      expect(screen.getByText('Test Beatmap 1')).toBeInTheDocument();
    });
  });

  it('should show loading spinner while fetching recommendations', async () => {
    (useGetCohort as any).mockReturnValue({
      isLoading: false,
      data: mockCohortData,
      error: null,
    });
    (useGetRecommendations as any).mockReturnValue({
      isLoading: true,
      data: null,
      error: null,
    });

    renderWithQueryClient(<App />);

    await waitFor(() => {
      expect(screen.getByText(/loading recommendations/i)).toBeInTheDocument();
    });
  });

  it('should display error message on API error', async () => {
    (useGetCohort as any).mockReturnValue({
      isLoading: false,
      data: null,
      error: new Error('Network error'),
    });

    renderWithQueryClient(<App />);

    await waitFor(() => {
      expect(screen.getByText(/failed to fetch cohort/i)).toBeInTheDocument();
    });
  });
});
