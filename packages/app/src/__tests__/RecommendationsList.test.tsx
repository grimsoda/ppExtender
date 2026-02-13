import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import RecommendationsList from '../components/RecommendationsList';
import { useGetRecommendations } from '../api-hooks';

// Mock the useGetRecommendations hook
vi.mock('../api-hooks', () => ({
  useGetRecommendations: vi.fn(),
}));

describe('RecommendationsList', () => {
  const mockRecommendations = [
    {
      beatmapId: 101,
      title: 'Test Beatmap 1',
      artist: 'Test Artist 1',
      difficulty: 'Hard',
      stars: 4.5,
      pp: 320,
      accuracy: 96.5,
      mods: ['HD', 'DT'],
      coverUrl: 'https://example.com/cover1.jpg',
    },
    {
      beatmapId: 102,
      title: 'Test Beatmap 2',
      artist: 'Test Artist 2',
      difficulty: 'Insane',
      stars: 5.2,
      pp: 380,
      accuracy: 94.2,
      mods: ['HR'],
      coverUrl: 'https://example.com/cover2.jpg',
    },
    {
      beatmapId: 103,
      title: 'Test Beatmap 3',
      artist: 'Test Artist 3',
      difficulty: 'Expert',
      stars: 6.1,
      pp: 450,
      accuracy: 92.8,
      mods: [],
      coverUrl: 'https://example.com/cover3.jpg',
    },
  ];

  const mockOnSelect = vi.fn();
  let queryClient: QueryClient;

  beforeEach(() => {
    mockOnSelect.mockClear();
    queryClient = new QueryClient({
      defaultOptions: {
        queries: {
          retry: false,
        },
      },
    });
    vi.clearAllMocks();
  });

  function renderWithProviders(ui: React.ReactElement) {
    return render(
      <QueryClientProvider client={queryClient}>
        {ui}
      </QueryClientProvider>
    );
  }

  it('should render list of recommended beatmaps', () => {
    vi.mocked(useGetRecommendations).mockReturnValue({
      data: mockRecommendations,
      isLoading: false,
      error: null,
    } as any);

    renderWithProviders(
      <RecommendationsList
        beatmapId={12345}
        onSelect={mockOnSelect}
      />
    );

    expect(screen.getByText(/recommendations/i)).toBeInTheDocument();
    expect(screen.getAllByTestId('beatmap-card')).toHaveLength(3);
  });

  it('should render beatmap titles', () => {
    vi.mocked(useGetRecommendations).mockReturnValue({
      data: mockRecommendations,
      isLoading: false,
      error: null,
    } as any);

    renderWithProviders(
      <RecommendationsList
        beatmapId={12345}
        onSelect={mockOnSelect}
      />
    );

    expect(screen.getByText('Test Beatmap 1')).toBeInTheDocument();
    expect(screen.getByText('Test Beatmap 2')).toBeInTheDocument();
    expect(screen.getByText('Test Beatmap 3')).toBeInTheDocument();
  });

  it('should render beatmap artists', () => {
    vi.mocked(useGetRecommendations).mockReturnValue({
      data: mockRecommendations,
      isLoading: false,
      error: null,
    } as any);

    renderWithProviders(
      <RecommendationsList
        beatmapId={12345}
        onSelect={mockOnSelect}
      />
    );

    expect(screen.getByText('Test Artist 1')).toBeInTheDocument();
    expect(screen.getByText('Test Artist 2')).toBeInTheDocument();
    expect(screen.getByText('Test Artist 3')).toBeInTheDocument();
  });

  it('should render difficulty and star ratings', () => {
    vi.mocked(useGetRecommendations).mockReturnValue({
      data: mockRecommendations,
      isLoading: false,
      error: null,
    } as any);

    renderWithProviders(
      <RecommendationsList
        beatmapId={12345}
        onSelect={mockOnSelect}
      />
    );

    expect(screen.getByText('Hard')).toBeInTheDocument();
    expect(screen.getByText('Insane')).toBeInTheDocument();
    expect(screen.getByText('Expert')).toBeInTheDocument();
    expect(screen.getByText((content) => content.includes('4.5'))).toBeInTheDocument();
    expect(screen.getByText((content) => content.includes('5.2'))).toBeInTheDocument();
    expect(screen.getByText((content) => content.includes('6.1'))).toBeInTheDocument();
  });

  it('should render pp and accuracy stats', () => {
    vi.mocked(useGetRecommendations).mockReturnValue({
      data: mockRecommendations,
      isLoading: false,
      error: null,
    } as any);

    renderWithProviders(
      <RecommendationsList
        beatmapId={12345}
        onSelect={mockOnSelect}
      />
    );

    expect(screen.getByText(/320.*pp/i)).toBeInTheDocument();
    expect(screen.getByText(/96\.5.*%/i)).toBeInTheDocument();
    expect(screen.getByText(/380.*pp/i)).toBeInTheDocument();
    expect(screen.getByText(/94\.2.*%/i)).toBeInTheDocument();
  });

  it('should render mod tags', () => {
    vi.mocked(useGetRecommendations).mockReturnValue({
      data: mockRecommendations,
      isLoading: false,
      error: null,
    } as any);

    renderWithProviders(
      <RecommendationsList
        beatmapId={12345}
        onSelect={mockOnSelect}
      />
    );

    expect(screen.getByText('HD')).toBeInTheDocument();
    expect(screen.getByText('DT')).toBeInTheDocument();
    expect(screen.getByText('HR')).toBeInTheDocument();
  });

  it('should handle beatmap selection', () => {
    vi.mocked(useGetRecommendations).mockReturnValue({
      data: mockRecommendations,
      isLoading: false,
      error: null,
    } as any);

    renderWithProviders(
      <RecommendationsList
        beatmapId={12345}
        onSelect={mockOnSelect}
      />
    );

    const firstCard = screen.getAllByTestId('beatmap-card')[0];
    fireEvent.click(firstCard);

    expect(mockOnSelect).toHaveBeenCalledWith(mockRecommendations[0]);
  });

  it('should render beatmap cover images', () => {
    vi.mocked(useGetRecommendations).mockReturnValue({
      data: mockRecommendations,
      isLoading: false,
      error: null,
    } as any);

    renderWithProviders(
      <RecommendationsList
        beatmapId={12345}
        onSelect={mockOnSelect}
      />
    );

    const images = screen.getAllByRole('img');
    expect(images).toHaveLength(3);
    expect(images[0]).toHaveAttribute('src', 'https://example.com/cover1.jpg');
  });

  it('should show empty state when no recommendations', () => {
    vi.mocked(useGetRecommendations).mockReturnValue({
      data: [],
      isLoading: false,
      error: null,
    } as any);

    renderWithProviders(
      <RecommendationsList
        beatmapId={12345}
        onSelect={mockOnSelect}
      />
    );

    expect(screen.getByText(/no recommendations found/i)).toBeInTheDocument();
  });

  it('should show loading state', () => {
    vi.mocked(useGetRecommendations).mockReturnValue({
      data: undefined,
      isLoading: true,
      error: null,
    } as any);

    renderWithProviders(
      <RecommendationsList
        beatmapId={12345}
        onSelect={mockOnSelect}
      />
    );

    expect(screen.getByText(/loading recommendations/i)).toBeInTheDocument();
    expect(screen.getByTestId('loading-spinner')).toBeInTheDocument();
  });

  it('should show error state', () => {
    vi.mocked(useGetRecommendations).mockReturnValue({
      data: undefined,
      isLoading: false,
      error: new Error('Failed to fetch'),
    } as any);

    renderWithProviders(
      <RecommendationsList
        beatmapId={12345}
        onSelect={mockOnSelect}
      />
    );

    expect(screen.getByText(/failed to load recommendations/i)).toBeInTheDocument();
  });

  it('should display recommendation count', () => {
    vi.mocked(useGetRecommendations).mockReturnValue({
      data: mockRecommendations,
      isLoading: false,
      error: null,
    } as any);

    renderWithProviders(
      <RecommendationsList
        beatmapId={12345}
        onSelect={mockOnSelect}
      />
    );

    expect(screen.getByText(/3.*recommendations?/i)).toBeInTheDocument();
  });
});
