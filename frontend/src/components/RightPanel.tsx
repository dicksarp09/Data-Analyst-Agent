import React from 'react'
import { useAnalysisStore } from '../hooks/useAnalysis'

export const RightPanel: React.FC = () => {
  const { selectedInsightId, trace, isLoading } = useAnalysisStore()
  
  // Only show when an insight is selected
  const shouldShow = selectedInsightId && !isLoading
  
  if (!shouldShow) {
    return (
      <div className="w-80 bg-gray-900 border-l border-gray-700 flex items-center justify-center">
        <p className="text-gray-500 text-sm">Select an insight to view evidence</p>
      </div>
    )
  }
  
  const selectedTrace = trace.filter(t => Object.keys(t).length > 0)
  
  return (
    <div className="w-80 bg-gray-900 border-l border-gray-700 flex flex-col">
      {/* Header */}
      <div className="p-4 border-b border-gray-700">
        <h3 className="text-gray-400 text-xs uppercase tracking-wide">
          Evidence & Trace
        </h3>
      </div>
      
      {/* Content */}
      <div className="flex-1 overflow-y-auto p-4 space-y-4">
        
        {/* Hypothesis info */}
        <div>
          <h4 className="text-xs text-gray-500 uppercase mb-2">Hypothesis</h4>
          <div className="bg-gray-800 rounded-lg p-3">
            <p className="text-gray-300 text-sm font-mono">
              {selectedInsightId || 'N/A'}
            </p>
          </div>
        </div>
        
        {/* Execution steps */}
        <div>
          <h4 className="text-xs text-gray-500 uppercase mb-2">Execution Steps</h4>
          <div className="space-y-1">
            {selectedTrace.map((step, index) => (
              <div key={index} className="bg-gray-800 rounded p-2">
                <p className="text-gray-400 text-xs">
                  <span className="text-blue-400">{step.phase}</span>
                  {step.status === 'completed' && (
                    <span className="text-green-400 ml-2">✓</span>
                  )}
                </p>
                <p className="text-gray-500 text-xs mt-1">
                  {Object.entries(step)
                    .filter(([k]) => k !== 'phase')
                    .map(([k, v]) => `${k}: ${typeof v === 'number' ? v.toLocaleString() : v}`)
                    .join(', ')}
                </p>
              </div>
            ))}
          </div>
        </div>
        
        {/* Stats */}
        <div>
          <h4 className="text-xs text-gray-500 uppercase mb-2">Summary</h4>
          <div className="grid grid-cols-2 gap-2">
            <div className="bg-gray-800 rounded p-2 text-center">
              <p className="text-green-400 text-lg font-bold">
                {trace.filter(t => t.accepted)?.reduce((sum, t) => sum + (t.accepted || 0), 0) || 0}
              </p>
              <p className="text-gray-500 text-xs">Accepted</p>
            </div>
            <div className="bg-gray-800 rounded p-2 text-center">
              <p className="text-red-400 text-lg font-bold">
                {trace.filter(t => t.rejected)?.reduce((sum, t) => sum + (t.rejected || 0), 0) || 0}
              </p>
              <p className="text-gray-500 text-xs">Rejected</p>
            </div>
          </div>
        </div>
        
      </div>
    </div>
  )
}