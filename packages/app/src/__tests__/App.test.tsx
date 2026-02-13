import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
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

vi.mock('../api', () => ({
  fetchCohort: vi.fn(),
  fetchRecommendations: vi.fn(),
  fetchAllPlays: vi.fn(),
}));

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
  };
});

import { fetchCohort, fetchRecommendations, fetchAllPlays } from '../api';

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
  });

  it('should render the app title', () => {
    renderWithQueryClient(<App />);

    expect(screen.getByText('osu! Recommender')).toBeInTheDocument();
  });

  it('should render SeedInput component', () => {
    renderWithQueryClient(<App />);
    
    expect(screen.getByLabelText(/beatmap id/i)).toBeInTheDocument();
  });

  it('should fetch cohort data on form submission', async () => {
    fetchCohort.mockResolvedValueOnce(mockCohortData);

    renderWithQueryClient(<App />);
    
    const input = screen.getByLabelText(/beatmap id/i);
    fireEvent.change(input, { target: { value: '12345' } });
    
    const submitButton = screen.getByRole('button', { name: /submit/i });
    fireEvent.click(submitButton);
    
    await waitFor(() => {
      expect(fetchCohort).toHaveBeenCalledWith({
        beatmapId: '12345',
        minPp: 0,
        maxPp: 500,
        mods: [],
        topK: 200,
      });
    });
  });

  it('should display cohort preview after successful fetch', async () => {
    fetchCohort.mockResolvedValueOnce(mockCohortData);
    
    renderWithQueryClient(<App />);
    
    const input = screen.getByLabelText(/beatmap id/i);
    fireEvent.change(input, { target: { value: '12345' } });
    
    const submitButton = screen.getByRole('button', { name: /submit/i });
    fireEvent.click(submitButton);
    
    await waitFor(() => {
      expect(screen.getByText(/cohort size/i)).toBeInTheDocument();
      expect(screen.getByText('150')).toBeInTheDocument();
    });
  });

  it('should fetch recommendations after cohort is loaded', async () => {
    fetchCohort.mockResolvedValueOnce(mockCohortData);
    fetchRecommendations.mockResolvedValueOnce(mockRecommendations);
    
    renderWithQueryClient(<App />);
    
    const input = screen.getByLabelText(/beatmap id/i);
    fireEvent.change(input, { target: { value: '12345' } });
    
    const submitButton = screen.getByRole('button', { name: /submit/i });
    fireEvent.click(submitButton);
    
    await waitFor(() => {
      expect(fetchRecommendations).toHaveBeenCalledWith({
        beatmapId: '12345',
        minPp: 0,
        maxPp: 500,
        mods: [],
        topK: 200,
      });
    });
  });

  it('should display recommendations after fetch', async () => {
    fetchCohort.mockResolvedValueOnce(mockCohortData);
    fetchRecommendations.mockResolvedValueOnce(mockRecommendations);
    
    renderWithQueryClient(<App />);
    
    const input = screen.getByLabelText(/beatmap id/i);
    fireEvent.change(input, { target: { value: '12345' } });
    
    const submitButton = screen.getByRole('button', { name: /submit/i });
    fireEvent.click(submitButton);
    
    await waitFor(() => {
      expect(screen.getByText('Test Beatmap 1')).toBeInTheDocument();
    });
  });

  it('should show loading spinner while fetching cohort', async () => {
    fetchCohort.mockImplementation(() => new Promise(() => {}));
    
    renderWithQueryClient(<App />);
    
    const input = screen.getByLabelText(/beatmap id/i);
    fireEvent.change(input, { target: { value: '12345' } });
    
    const submitButton = screen.getByRole('button', { name: /submit/i });
    fireEvent.click(submitButton);
    
    expect(screen.getByTestId('loading-spinner')).toBeInTheDocument();
  });

  it('should show loading spinner while fetching recommendations', async () => {
    fetchCohort.mockResolvedValueOnce(mockCohortData);
    fetchRecommendations.mockImplementation(() => new Promise(() => {}));
    
    renderWithQueryClient(<App />);
    
    const input = screen.getByLabelText(/beatmap id/i);
    fireEvent.change(input, { target: { value: '12345' } });
    
    const submitButton = screen.getByRole('button', { name: /submit/i });
    fireEvent.click(submitButton);
    
    await waitFor(() => {
      expect(screen.getByText(/loading recommendations/i)).toBeInTheDocument();
    });
  });

  it('should display error message on invalid beatmap_id', async () => {
    fetchCohort.mockRejectedValueOnce(new Error('Invalid beatmap ID'));
    
    renderWithQueryClient(<App />);
    
    const input = screen.getByLabelText(/beatmap id/i);
    fireEvent.change(input, { target: { value: 'invalid' } });
    
    const submitButton = screen.getByRole('button', { name: /submit/i });
    fireEvent.click(submitButton);
    
    await waitFor(() => {
      expect(screen.getByText(/invalid beatmap id/i)).toBeInTheDocument();
    });
  });

  it('should display error message on API error', async () => {
    fetchCohort.mockRejectedValueOnce(new Error('Network error'));
    
    renderWithQueryClient(<App />);
    
    const input = screen.getByLabelText(/beatmap id/i);
    fireEvent.change(input, { target: { value: '12345' } });
    
    const submitButton = screen.getByRole('button', { name: /submit/i });
    fireEvent.click(submitButton);
    
    await waitFor(() => {
      expect(screen.getByText(/failed to fetch cohort/i)).toBeInTheDocument();
    });
  });

  it('should handle mod selection in form', async () => {
    fetchCohort.mockResolvedValueOnce(mockCohortData);
    
    renderWithQueryClient(<App />);
    
    const input = screen.getByLabelText(/beatmap id/i);
    fireEvent.change(input, { target: { value: '12345' } });
    
    const hdCheckbox = screen.getByLabelText(/hd/i);
    fireEvent.click(hdCheckbox);
    
    const submitButton = screen.getByRole('button', { name: /submit/i });
    fireEvent.click(submitButton);
    
    await waitFor(() => {
      expect(fetchCohort).toHaveBeenCalledWith(expect.objectContaining({
        mods: ['HD'],
      }));
    });
  });

  it('should handle pp range selection in form', async () => {
    fetchCohort.mockResolvedValueOnce(mockCohortData);
    
    renderWithQueryClient(<App />);
    
    const input = screen.getByLabelText(/beatmap id/i);
    fireEvent.change(input, { target: { value: '12345' } });
    
    const minPpSlider = screen.getByLabelText(/min pp/i);
    fireEvent.change(minPpSlider, { target: { value: '200' } });
    
    const submitButton = screen.getByRole('button', { name: /submit/i });
    fireEvent.click(submitButton);
    
    await waitFor(() => {
      expect(fetchCohort).toHaveBeenCalledWith(expect.objectContaining({
        minPp: 200,
      }));
    });
  });
});
