import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import CohortPreview from '../components/CohortPreview';

describe('CohortPreview', () => {
  const mockCohortData = {
    size: 150,
    ppDistribution: {
      min: 180,
      max: 450,
      mean: 320,
      median: 310,
    },
    accuracyDistribution: {
      min: 85.5,
      max: 99.8,
      mean: 94.2,
      median: 95.1,
    },
    topPlayers: [
      { userId: 1, username: 'Player1', pp: 350, accuracy: 99.5 },
      { userId: 2, username: 'Player2', pp: 320, accuracy: 98.2 },
      { userId: 3, username: 'Player3', pp: 280, accuracy: 97.8 },
    ],
    plays: [
      { userId: 1, pp: 350, accuracy: 99.5, score: 1000000, mods: '[]', rank: 'X' },
      { userId: 2, pp: 320, accuracy: 98.2, score: 950000, mods: '[]', rank: 'S' },
      { userId: 3, pp: 280, accuracy: 97.8, score: 900000, mods: '[]', rank: 'A' },
    ],
    seedPpRange: {
      lower: 200,
      upper: 400,
    },
  };

  it('should render cohort size', () => {
    render(<CohortPreview cohort={mockCohortData} />);
    
    expect(screen.getByText(/cohort size/i)).toBeInTheDocument();
    expect(screen.getByText('150')).toBeInTheDocument();
  });

  it('should render pp vs accuracy scatter plot', () => {
    render(<CohortPreview cohort={mockCohortData} />);
    
    expect(screen.getByText(/pp vs accuracy distribution/i)).toBeInTheDocument();
    expect(screen.getByTestId('pp-accuracy-scatter')).toBeInTheDocument();
  });

  it('should render pp and accuracy ranges', () => {
    render(<CohortPreview cohort={mockCohortData} />);
    
    expect(screen.getByText(/pp range/i)).toBeInTheDocument();
    expect(screen.getByText(/accuracy range/i)).toBeInTheDocument();
  });

  it('should render legend', () => {
    render(<CohortPreview cohort={mockCohortData} />);

    expect(screen.getByText(/legend/i)).toBeInTheDocument();
    expect(screen.getByText('SS')).toBeInTheDocument();
    expect(screen.getByText('S')).toBeInTheDocument();
    expect(screen.getByText('A')).toBeInTheDocument();
    expect(screen.getByText(/\+CL \(Classic mod\)/)).toBeInTheDocument();
  });

  it('should render top players list', () => {
    render(<CohortPreview cohort={mockCohortData} />);
    
    expect(screen.getByText(/top players/i)).toBeInTheDocument();
    expect(screen.getByText('Player1')).toBeInTheDocument();
    expect(screen.getByText('Player2')).toBeInTheDocument();
    expect(screen.getByText('Player3')).toBeInTheDocument();
  });

  it('should display player stats correctly', () => {
    render(<CohortPreview cohort={mockCohortData} />);

    expect(screen.getByText(/350.*pp/i)).toBeInTheDocument();
    expect(screen.getByText(/99.5/i)).toBeInTheDocument();
  });

  it('should show empty state when no cohort data', () => {
    render(<CohortPreview cohort={null} />);
    
    expect(screen.getByText(/no cohort data available/i)).toBeInTheDocument();
  });

  it('should show loading state', () => {
    render(<CohortPreview cohort={null} isLoading={true} />);
    
    expect(screen.getByText(/loading cohort data/i)).toBeInTheDocument();
    expect(screen.getByTestId('loading-spinner')).toBeInTheDocument();
  });

  it('should show empty top players message when no players', () => {
    const noPlayersData = { ...mockCohortData, topPlayers: [] };
    render(<CohortPreview cohort={noPlayersData} />);
    
    expect(screen.getByText(/no top players data available/i)).toBeInTheDocument();
  });
});
