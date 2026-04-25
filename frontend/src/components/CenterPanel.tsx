import React from 'react'
import { useAnalysisStore } from '../hooks/useAnalysis'
import { InsightList } from './InsightCard'
import { PlotGrid } from './PlotGrid'

export const CenterPanel: React.FC = () => {
  const { selectedInsightId, datasetMeta, isLoading } = useAnalysisStore()
  
  return (
    <div className="flex-1 flex flex-col min-h-0">
      {/* Dataset meta bar */}
      {datasetMeta && (
        <div className="flex items-center gap-4 px-4 py-2 bg-gray-800/50 border-b border-gray-700">
          <span className="text-gray-400 text-sm">
            <span className="text-gray-500">Dataset:</span>{' '}
            {datasetMeta.rows.toLocaleString()} rows ×{' '}
            {datasetMeta.columns} columns
          </span>
          <span className="text-gray-600">|</span>
          <span className="text-green-400 text-sm">Analysis complete</span>
        </div>
      )}
      
      {/* Main content - split view */}
      <div className="flex-1 flex min-h-0">
        {/* Left: Insight list - 35% */}
        <div className="w-[35%] border-r border-gray-700 overflow-y-auto p-4">
          <InsightList />
        </div>
        
        {/* Right: Active insight + plots - 65% */}
        <div className="flex-1 overflow-y-auto p-4">
          {selectedInsightId ? (
            <div className="space-y-6">
              <SelectedInsightHeader />
              <PlotGrid />
            </div>
          ) : (
            <div className="flex flex-col items-center justify-center h-full text-gray-500">
              <p>Select an insight to view details</p>
            </div>
          )}
        </div>
      </div>
    </div>
  )
}

const SelectedInsightHeader: React.FC = () => {
  const { getSelectedInsight } = useAnalysisStore()
  const insight = getSelectedInsight()
  
  if (!insight) return null
  
  const confidence = Math.round((insight.confidence || 0) * 100)
  
  return (
    <div className="space-y-2">
      <div className="flex items-center justify-between">
        <span className="text-gray-400 text-xs uppercase tracking-wide">
          Selected Insight
        </span>
        <span className={`
          px-2 py-1 rounded text-xs font-bold
          ${confidence >= 75 ? 'bg-green-500/20 text-green-400' : 
            confidence >= 50 ? 'bg-yellow-500/20 text-yellow-400' : 
            'bg-red-500/20 text-red-400'}
        `}>
          {confidence}% confidence
        </span>
      </div>
      
      <p className="text-gray-200 text-lg">
        {insight.insight || insight.claim || 'No description'}
      </p>
      
      <div className="flex items-center gap-3 text-sm text-gray-500">
        <span className="uppercase">{insight.type || 'insight'}</span>
        {insight.hypothesis_id && (
          <>
            <span>•</span>
            <span className="font-mono text-xs">{insight.hypothesis_id}</span>
          </>
        )}
      </div>
    </div>
  )
}