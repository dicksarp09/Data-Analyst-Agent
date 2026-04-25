import React, { useState } from 'react'
import { Send, Loader2, Sparkles } from 'lucide-react'
import { useAnalysisStore } from '../hooks/useAnalysis'
import { askQuestion,runSimulation } from '../api/client'

const suggestions = [
  "Why did revenue drop?",
  "Compare mobile vs desktop conversion",
  "Show anomalies in the data",
  "What drives user conversion?",
  "Find unusual patterns"
]

export const LeftPanel: React.FC = () => {
  const { sessionId, viewMode, setViewMode, setInsights, setPlots, selectInsight, setLoading, isLoading } = useAnalysisStore()
  const [question, setQuestion] = useState('')
  
  const handleAsk = async () => {
    if (!question.trim() || !sessionId || isLoading) return
    
    setLoading(true, 'Analyzing your question...')
    
    try {
      const result = await askQuestion(question, sessionId)
      
      // Update insights if returned
      if (result.insights && result.insights.length > 0) {
        setInsights(result.insights)
        
        // Auto-select top insight
        selectInsight(result.insights[0].id)
        setViewMode('chat')
      }
      
      // Update plots if returned
      if (result.plots) {
        setPlots(result.plots)
      }
    } catch (error) {
      console.error('NLQ error:', error)
    }
    
    setLoading(false)
  }
  
  const handleSuggestion = (suggestion: string) => {
    setQuestion(suggestion)
  }
  
  // Simulation mode
  const [simVariable, setSimVariable] = useState('')
  const [simChange, setSimChange] = useState(0)
  const [simResult, setSimResult] = useState<any>(null)
  
  const handleSimulate = async () => {
    if (!sessionId || !simVariable) return
    
    setLoading(true, 'Running simulation...')
    
    try {
      const result = await runSimulation(sessionId, simVariable, simChange)
      setSimResult(result)
    } catch (error) {
      console.error('Simulation error:', error)
    }
    
    setLoading(false)
  }
  
  return (
    <div className="w-72 bg-gray-900 border-r border-gray-700 flex flex-col">
      {/* Mode tabs */}
      <div className="flex border-b border-gray-700">
        <button
          onClick={() => setViewMode('insight')}
          className={`flex-1 py-2 text-xs font-medium ${
            viewMode === 'insight' ? 'text-blue-400 border-b-2 border-blue-400' : 'text-gray-500'
          }`}
        >
          Analyze
        </button>
        <button
          onClick={() => setViewMode('simulation')}
          className={`flex-1 py-2 text-xs font-medium ${
            viewMode === 'simulation' ? 'text-blue-400 border-b-2 border-blue-400' : 'text-gray-500'
          }`}
        >
          Simulate
        </button>
      </div>
      
      {/* Content based on mode */}
      <div className="flex-1 overflow-y-auto p-4 space-y-4">
        
        {viewMode === 'insight' ? (
          <>
            {/* NLQ Input */}
            <div>
              <label className="text-xs text-gray-500 uppercase tracking-wide mb-2 block">
                Ask about your data
              </label>
              <div className="relative">
                <textarea
                  value={question}
                  onChange={(e) => setQuestion(e.target.value)}
                  onKeyDown={(e) => e.key === 'Enter' && !e.shiftKey && handleAsk()}
                  placeholder="What do you want to know?"
                  className="w-full bg-gray-800 border border-gray-700 rounded-lg p-3 text-gray-200 text-sm resize-none h-24 focus:border-blue-500 focus:outline-none"
                />
                <button
                  onClick={handleAsk}
                  disabled={!question.trim() || !sessionId || isLoading}
                  className="absolute bottom-2 right-2 p-2 bg-blue-500 rounded-lg text-white disabled:opacity-50 disabled:cursor-not-allowed"
                >
                  {isLoading ? (
                    <Loader2 className="w-4 h-4 animate-spin" />
                  ) : (
                    <Send className="w-4 h-4" />
                  )}
                </button>
              </div>
            </div>
            
            {/* Suggestions */}
            <div>
              <label className="text-xs text-gray-500 uppercase tracking-wide mb-2 block">
                Suggestions
              </label>
              <div className="space-y-1">
                {suggestions.map((suggestion) => (
                  <button
                    key={suggestion}
                    onClick={() => handleSuggestion(suggestion)}
                    className="w-full text-left px-3 py-2 text-sm text-gray-400 hover:text-gray-200 hover:bg-gray-800 rounded transition-colors"
                  >
                    {suggestion}
                  </button>
                ))}
              </div>
            </div>
          </>
        ) : (
          /* Simulation mode */
          <div className="space-y-4">
            <div>
              <label className="text-xs text-gray-500 uppercase tracking-wide mb-2 block">
                Variable to adjust
              </label>
              <input
                type="text"
                value={simVariable}
                onChange={(e) => setSimVariable(e.target.value)}
                placeholder="e.g., latency_ms, revenue"
                className="w-full bg-gray-800 border border-gray-700 rounded-lg px-3 py-2 text-gray-200 text-sm"
              />
            </div>
            
            <div>
              <label className="text-xs text-gray-500 uppercase tracking-wide mb-2 block">
                Change %
              </label>
              <input
                type="range"
                min="-50"
                max="50"
                value={simChange}
                onChange={(e) => setSimChange(Number(e.target.value))}
                className="w-full"
              />
              <div className="text-center text-gray-300">
                {simChange > 0 ? '+' : ''}{simChange}%
              </div>
            </div>
            
            <button
              onClick={handleSimulate}
              disabled={!simVariable || isLoading}
              className="w-full py-2 bg-blue-500 text-white rounded-lg text-sm font-medium disabled:opacity-50"
            >
              Run Simulation
            </button>
            
            {simResult && (
              <div className="bg-gray-800 rounded-lg p-3">
                <p className="text-xs text-gray-500 mb-1">Prediction</p>
                {simResult.success ? (
                  <>
                    <p className="text-gray-200 text-lg">
                      {simResult.predicted_value?.toFixed(2)}
                    </p>
                    <p className="text-green-400 text-sm">
                      {simResult.change_percent > 0 ? '+' : ''}{simResult.change_percent}% from baseline
                    </p>
                  </>
                ) : (
                  <p className="text-red-400 text-sm">{simResult.error}</p>
                )}
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  )
}