import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import RecommendationsList from '../components/RecommendationsList';

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

  beforeEach(() => {
    mockOnSelect.mockClear();
  });

  it('should render list of recommended beatmaps', () => {
    render(
      <RecommendationsList 
        recommendations={mockRecommendations} 
        onSelect={mockOnSelect}
      />
    );
    
    expect(screen.getByText(/recommendations/i)).toBeInTheDocument();
    expect(screen.getAllByTestId('beatmap-card')).toHaveLength(3);
  });

  it('should render beatmap titles', () => {
    render(
      <RecommendationsList 
        recommendations={mockRecommendations} 
        onSelect={mockOnSelect}
      />
    );
    
    expect(screen.getByText('Test Beatmap 1')).toBeInTheDocument();
    expect(screen.getByText('Test Beatmap 2')).toBeInTheDocument();
    expect(screen.getByText('Test Beatmap 3')).toBeInTheDocument();
  });

  it('should render beatmap artists', () => {
    render(
      <RecommendationsList 
        recommendations={mockRecommendations} 
        onSelect={mockOnSelect}
      />
    );
    
    expect(screen.getByText('Test Artist 1')).toBeInTheDocument();
    expect(screen.getByText('Test Artist 2')).toBeInTheDocument();
    expect(screen.getByText('Test Artist 3')).toBeInTheDocument();
  });

  it('should render difficulty and star ratings', () => {
    render(
      <RecommendationsList 
        recommendations={mockRecommendations} 
        onSelect={mockOnSelect}
      />
    );
    
    expect(screen.getByText(/hard.*4\.5/i)).toBeInTheDocument();
    expect(screen.getByText(/insane.*5\.2/i)).toBeInTheDocument();
    expect(screen.getByText(/expert.*6\.1/i)).toBeInTheDocument();
  });

  it('should render pp and accuracy stats', () => {
    render(
      <RecommendationsList 
        recommendations={mockRecommendations} 
        onSelect={mockOnSelect}
      />
    );
    
    expect(screen.getByText(/320.*pp/i)).toBeInTheDocument();
    expect(screen.getByText(/96\.5.*%/i)).toBeInTheDocument();
    expect(screen.getByText(/380.*pp/i)).toBeInTheDocument();
    expect(screen.getByText(/94\.2.*%/i)).toBeInTheDocument();
  });

  it('should render mod tags', () => {
    render(
      <RecommendationsList 
        recommendations={mockRecommendations} 
        onSelect={mockOnSelect}
      />
    );
    
    expect(screen.getByText('HD')).toBeInTheDocument();
    expect(screen.getByText('DT')).toBeInTheDocument();
    expect(screen.getByText('HR')).toBeInTheDocument();
  });

  it('should handle beatmap selection', () => {
    render(
      <RecommendationsList 
        recommendations={mockRecommendations} 
        onSelect={mockOnSelect}
      />
    );
    
    const firstCard = screen.getAllByTestId('beatmap-card')[0];
    fireEvent.click(firstCard);
    
    expect(mockOnSelect).toHaveBeenCalledWith(mockRecommendations[0]);
  });

  it('should render beatmap cover images', () => {
    render(
      <RecommendationsList 
        recommendations={mockRecommendations} 
        onSelect={mockOnSelect}
      />
    );
    
    const images = screen.getAllByRole('img');
    expect(images).toHaveLength(3);
    expect(images[0]).toHaveAttribute('src', 'https://example.com/cover1.jpg');
  });

  it('should show empty state when no recommendations', () => {
    render(
      <RecommendationsList 
        recommendations={[]} 
        onSelect={mockOnSelect}
      />
    );
    
    expect(screen.getByText(/no recommendations found/i)).toBeInTheDocument();
  });

  it('should show loading state', () => {
    render(
      <RecommendationsList 
        recommendations={[]}
        onSelect={mockOnSelect}
        isLoading={true}
      />
    );
    
    expect(screen.getByText(/loading recommendations/i)).toBeInTheDocument();
    expect(screen.getByTestId('loading-spinner')).toBeInTheDocument();
  });

  it('should display recommendation count', () => {
    render(
      <RecommendationsList 
        recommendations={mockRecommendations} 
        onSelect={mockOnSelect}
      />
    );
    
    expect(screen.getByText(/3.*recommendations?/i)).toBeInTheDocument();
  });
});
