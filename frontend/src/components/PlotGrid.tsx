import React, { useEffect, useRef } from 'react'
import Plot from 'react-plotly.js'
import { useAnalysisStore } from '../hooks/useAnalysis'

export const PlotGrid: React.FC = () => {
  const { getInsightPlots, getSelectedInsight } = useAnalysisStore()
  const insight = getSelectedInsight()
  const plots = getInsightPlots()
  
  if (!insight || plots.length === 0) {
    return (
      <div className="flex flex-col items-center justify-center h-64 text-gray-500">
        <p>No plots for this insight</p>
      </div>
    )
  }
  
  return (
    <div className="space-y-4">
      <h3 className="text-gray-400 text-xs uppercase tracking-wide">
        Related Visualizations
      </h3>
      
      {plots.map((plot) => (
        <div key={plot.plot_id} className="bg-gray-900 rounded-lg p-2">
          <p className="text-gray-300 text-sm mb-2">{plot.title || plot.plot_id}</p>
          
          {plot.data ? (
            // Base64 image from backend
            <img 
              src={`data:image/png;base64,${plot.data}`} 
              alt={plot.title}
              className="w-full rounded"
            />
          ) : plot.x && plot.y ? (
            // Plotly data
            <Plot
              data={[
                {
                  x: plot.x,
                  y: plot.y,
                  type: plot.type || 'scatter',
                  mode: 'lines+markers',
                  marker: { color: '#60a5fa' },
                  line: { color: '#60a5fa' }
                }
              ]}
              layout={{
                paper_bgcolor: 'transparent',
                plot_bgcolor: 'transparent',
                font: { color: '#9ca3af' },
                margin: { t: 20, r: 20, l: 40, b: 40 },
                xaxis: { 
                  gridcolor: '#374151',
                  zerolinecolor: '#4b5563'
                },
                yaxis: { 
                  gridcolor: '#374151',
                  zerolinecolor: '#4b5563'
                }
              }}
              config={{ displayModeBar: false }}
              style={{ width: '100%', height: '250px' }}
            />
          ) : (
            <div className="h-32 flex items-center justify-center text-gray-500">
              No plot data available
            </div>
          )}
        </div>
      ))}
    </div>
  )
}