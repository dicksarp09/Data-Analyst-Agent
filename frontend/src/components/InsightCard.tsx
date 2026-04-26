import React from 'react'
import { Insight, useAnalysisStore } from '../hooks/useAnalysis'

interface InsightCardProps {
  insight: Insight
  isSelected: boolean
  onClick: () => void
}

export const InsightCard: React.FC<InsightCardProps> = ({ insight, isSelected, onClick }) => {
  const confidence = insight.confidence || 0
  const confidencePercent = Math.round(confidence * 100)
  
  // Color based on confidence
  const confidenceColor = confidence >= 0.75 
    ? 'text-green-400' 
    : confidence >= 0.5 
      ? 'text-yellow-400' 
      : 'text-red-400'
  
  const confidenceRing = confidence >= 0.75 
    ? 'border-green-500/50' 
    : confidence >= 0.5 
      ? 'border-yellow-500/50' 
      : 'border-red-500/50'
  
  return (
    <button
      onClick={onClick}
      className={`
        w-full text-left p-4 rounded-lg border transition-all
        ${isSelected 
          ? 'bg-blue-500/20 border-blue-500' 
          : 'bg-gray-800/50 border-gray-700 hover:border-gray-600'
        }
      `}
    >
      <div className="flex items-start justify-between gap-3">
        <div className="flex-1">
          <p className="text-gray-200 text-sm line-clamp-2">
            {insight.title || insight.insight || insight.summary || insight.claim || 'No description'}
          </p>
          
          <div className="flex items-center gap-2 mt-2">
            <span className="text-xs text-gray-500 uppercase">
              {insight.type || insight.impact || 'insight'}
            </span>
            {((insight.plot_ids && insight.plot_ids.length) || (insight.supporting_plots && insight.supporting_plots.length)) > 0 && (
              <span className="text-xs text-gray-500">
                • { (insight.plot_ids || insight.supporting_plots || []).length } plot{((insight.plot_ids || insight.supporting_plots || []).length > 1 ? 's' : '')}
              </span>
            )}
          </div>
        </div>
        
        {/* Confidence circle */}
        <div className={`flex-shrink-0 w-10 h-10 rounded-full border-2 ${confidenceRing} flex items-center justify-center`}>
          <span className={`text-xs font-bold ${confidenceColor}`}>
            {confidencePercent}%
          </span>
        </div>
      </div>
    </button>
  )
}

export const InsightList: React.FC = () => {
  const { insights, selectedInsightId, selectInsight } = useAnalysisStore()
  
  if (insights.length === 0) {
    return (
      <div className="flex flex-col items-center justify-center h-64 text-gray-500">
        <p>No insights yet</p>
        <p className="text-sm">Upload a dataset to get started</p>
      </div>
    )
  }
  
  return (
    <div className="space-y-2">
      <p className="text-gray-400 text-xs uppercase tracking-wide mb-3">
        Key Insights ({insights.length})
      </p>
      
      {insights.map((insight) => (
        <InsightCard
          key={insight.id}
          insight={insight}
          isSelected={insight.id === selectedInsightId}
          onClick={() => selectInsight(insight.id)}
        />
      ))}
    </div>
  )
}