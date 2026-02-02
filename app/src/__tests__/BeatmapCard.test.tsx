import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import BeatmapCard from '../components/BeatmapCard';

describe('BeatmapCard', () => {
  const mockBeatmap = {
    beatmapId: 101,
    title: 'Test Beatmap',
    artist: 'Test Artist',
    difficulty: 'Hard',
    stars: 4.5,
    pp: 320,
    accuracy: 96.5,
    mods: ['HD', 'DT'],
    coverUrl: 'https://example.com/cover.jpg',
  };

  const mockOnClick = vi.fn();

  beforeEach(() => {
    mockOnClick.mockClear();
  });

  it('should render beatmap title', () => {
    render(<BeatmapCard beatmap={mockBeatmap} onClick={mockOnClick} />);
    
    expect(screen.getByText('Test Beatmap')).toBeInTheDocument();
  });

  it('should render beatmap artist', () => {
    render(<BeatmapCard beatmap={mockBeatmap} onClick={mockOnClick} />);
    
    expect(screen.getByText('Test Artist')).toBeInTheDocument();
  });

  it('should render difficulty name', () => {
    render(<BeatmapCard beatmap={mockBeatmap} onClick={mockOnClick} />);
    
    expect(screen.getByText('Hard')).toBeInTheDocument();
  });

  it('should render star rating', () => {
    render(<BeatmapCard beatmap={mockBeatmap} onClick={mockOnClick} />);
    
    expect(screen.getByText(/4\.5/i)).toBeInTheDocument();
  });

  it('should render pp value', () => {
    render(<BeatmapCard beatmap={mockBeatmap} onClick={mockOnClick} />);
    
    expect(screen.getByText(/320.*pp/i)).toBeInTheDocument();
  });

  it('should render accuracy percentage', () => {
    render(<BeatmapCard beatmap={mockBeatmap} onClick={mockOnClick} />);
    
    expect(screen.getByText(/96\.5.*%/i)).toBeInTheDocument();
  });

  it('should render mod tags', () => {
    render(<BeatmapCard beatmap={mockBeatmap} onClick={mockOnClick} />);
    
    expect(screen.getByText('HD')).toBeInTheDocument();
    expect(screen.getByText('DT')).toBeInTheDocument();
  });

  it('should render beatmap cover image', () => {
    render(<BeatmapCard beatmap={mockBeatmap} onClick={mockOnClick} />);
    
    const image = screen.getByRole('img');
    expect(image).toHaveAttribute('src', 'https://example.com/cover.jpg');
    expect(image).toHaveAttribute('alt', 'Test Beatmap');
  });

  it('should handle click events', () => {
    render(<BeatmapCard beatmap={mockBeatmap} onClick={mockOnClick} />);
    
    const card = screen.getByTestId('beatmap-card');
    fireEvent.click(card);
    
    expect(mockOnClick).toHaveBeenCalledWith(mockBeatmap);
  });

  it('should render with correct data-testid', () => {
    render(<BeatmapCard beatmap={mockBeatmap} onClick={mockOnClick} />);
    
    expect(screen.getByTestId('beatmap-card')).toBeInTheDocument();
  });

  it('should render without mods when mods array is empty', () => {
    const beatmapWithoutMods = { ...mockBeatmap, mods: [] };
    render(<BeatmapCard beatmap={beatmapWithoutMods} onClick={mockOnClick} />);
    
    expect(screen.queryByTestId('mod-tag')).not.toBeInTheDocument();
  });

  it('should display beatmap ID', () => {
    render(<BeatmapCard beatmap={mockBeatmap} onClick={mockOnClick} />);
    
    expect(screen.getByText(/#101/i)).toBeInTheDocument();
  });
});
