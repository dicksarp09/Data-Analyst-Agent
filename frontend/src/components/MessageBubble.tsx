import React, { useState } from 'react'
import { 
  Bot, 
  ChevronDown, 
  ChevronUp, 
  BarChart3, 
  TrendingUp, 
  AlertTriangle,
  Database,
  Clock,
  CheckCircle2,
  XCircle,
  Sparkles
} from 'lucide-react'
import type { ChatMessage } from '../hooks/useAnalysis'

interface MessageBubbleProps {
  message: ChatMessage
  isExpanded: boolean
  onToggleExpand: () => void
}

const typeLabels: Record<string, { label: string; icon: React.ReactNode; color: string }> = {
  causal: { label: 'Relationship', icon: <TrendingUp className="w-3 h-3" />, color: 'text-amber-400' },
  'segment-based': { label: 'Segment', icon: <Database className="w-3 h-3" />, color: 'text-cyan-400' },
  temporal: { label: 'Trend', icon: <Clock className="w-3 h-3" />, color: 'text-purple-400' },
  'data-quality': { label: 'Quality', icon: <AlertTriangle className="w-3 h-3" />, color: 'text-red-400' },
}

const confidenceColors = {
  high: 'bg-emerald-500/20 text-emerald-400 border-emerald-500/30',
  medium: 'bg-amber-500/20 text-amber-400 border-amber-500/30',
  low: 'bg-red-500/20 text-red-400 border-red-500/30',
}

function getConfidenceLevel(confidence: number): 'high' | 'medium' | 'low' {
  if (confidence >= 0.7) return 'high'
  if (confidence >= 0.5) return 'medium'
  return 'low'
}

const explanations: Record<string, string> = {
  causal: "This means there's a statistical relationship between these variables - when one changes, the other tends to change too.",
  'segment-based': "This shows that different groups of users behave differently. Each segment has unique characteristics.",
  temporal: "This pattern changed over time, suggesting external factors or seasonality affected the data.",
  'data-quality': "This might be due to how the data was collected rather than a real pattern. Consider verifying the source.",
}

export const MessageBubble: React.FC<MessageBubbleProps> = ({ message, isExpanded, onToggleExpand }) => {
  const isUser = message.role === 'user'
  
  const analysis = message.analysis

  if (isUser) {
    return (
      <div className="animate-slide-in mb-4 flex justify-end">
        <div className="max-w-[80%] bg-[var(--user-bubble)] border border-[var(--border-subtle)] rounded-2xl px-4 py-3">
          <p className="text-[var(--text-primary)] text-sm">{message.content}</p>
          <p className="text-[var(--text-dim)] text-xs mt-1 opacity-50">
            {new Date(message.timestamp).toLocaleTimeString()}
          </p>
        </div>
      </div>
    )
  }

  return (
    <div className="animate-fade-in-up mb-4">
      <div className="flex items-start gap-3">
        <div className="w-8 h-8 rounded-full bg-[var(--accent-primary)]/20 flex items-center justify-center flex-shrink-0 mt-1">
          <Bot className="w-4 h-4 text-[var(--accent-primary)]" />
        </div>
        
        <div className="flex-1 min-w-0">
          <div className="bg-[var(--ai-bubble)] border border-[var(--border-subtle)] rounded-2xl overflow-hidden">
            <div className="px-4 py-3 border-b border-[var(--border-subtle)] flex items-center justify-between">
              <span className="text-[var(--text-muted)] text-xs flex items-center gap-2">
                <Sparkles className="w-3 h-3" />
                Data Analyst
              </span>
              {analysis?.confidence !== undefined && (
                <span className={`px-2 py-0.5 rounded-full text-xs border ${confidenceColors[getConfidenceLevel(analysis.confidence)]}`}>
                  {Math.round(analysis.confidence * 100)}% confidence
                </span>
              )}
            </div>
            
            <div className="p-4">
              <p className="text-[var(--text-primary)] text-sm leading-relaxed whitespace-pre-wrap">
                {message.content}
              </p>
              
              {analysis?.insights && analysis.insights.length > 0 && (
                <div className="mt-4 pt-4 border-t border-[var(--border-subtle)]">
                  <p className="text-[var(--text-muted)] text-xs mb-3 flex items-center gap-2">
                    <BarChart3 className="w-3 h-3" />
                    Key Findings
                  </p>
                  
                  <div className="space-y-2">
                    {analysis.insights.slice(0, 3).map((insight: any, idx: number) => {
                      const typeInfo = typeLabels[insight.type] || { label: 'Finding', icon: null, color: 'text-[var(--text-muted)]' }
                      return (
                        <div 
                          key={insight.id || idx}
                          className="flex items-start gap-2 p-2 rounded-lg bg-[var(--bg-secondary)]/50"
                        >
                          <span className={typeInfo.color}>{typeInfo.icon}</span>
                          <div className="flex-1 min-w-0">
                            <p className="text-[var(--text-secondary)] text-xs">{insight.insight}</p>
                            {explanations[insight.type] && (
                              <p className="text-[var(--text-dim)] text-xs mt-1">{explanations[insight.type]}</p>
                            )}
                          </div>
                          <span className={`text-xs ${confidenceColors[getConfidenceLevel(insight.confidence)]} px-1.5 py-0.5 rounded`}>
                            {Math.round(insight.confidence * 100)}%
                          </span>
                        </div>
                      )
                    })}
                  </div>
                </div>
              )}
            </div>
            
            {(analysis?.insights?.length || analysis?.plots?.length) && (
              <div className="px-4 py-3 bg-[var(--bg-secondary)]/30 border-t border-[var(--border-subtle)]">
                <button
                  onClick={onToggleExpand}
                  className="flex items-center gap-2 text-[var(--text-muted)] text-xs hover:text-[var(--text-primary)] transition-colors"
                >
                  {isExpanded ? (
                    <>
                      <ChevronUp className="w-3 h-3" />
                      Hide Details
                    </>
                  ) : (
                    <>
                      <ChevronDown className="w-3 h-3" />
                      View Details ({analysis.insights?.length || 0} insights, {analysis.plots?.length || 0} plots)
                    </>
                  )}
                </button>
                
                {isExpanded && (
                  <div className="mt-4 space-y-4 animate-fade-in-up">
                    {analysis?.insights && analysis.insights.length > 0 && (
                      <div>
                        <p className="text-[var(--text-muted)] text-xs mb-2 flex items-center gap-2">
                          <BarChart3 className="w-3 h-3" />
                          All Insights
                        </p>
                        <div className="space-y-2">
                          {analysis.insights.map((insight: any, idx: number) => {
                            const typeInfo = typeLabels[insight.type] || { label: 'Insight', icon: null, color: 'text-[var(--text-muted)]' }
                            return (
                              <div 
                                key={insight.id || idx}
                                className="flex items-start gap-3 p-3 rounded-lg bg-[var(--bg-primary)] border border-[var(--border-subtle)]"
                              >
                                <div className="flex-1">
                                  <div className="flex items-center gap-2 mb-1">
                                    <span className={typeInfo.color}>{typeInfo.icon}</span>
                                    <span className="text-[var(--text-secondary)] text-xs font-medium">{typeInfo.label}</span>
                                    <span className="text-[var(--text-dim)] text-xs">•</span>
                                    <span className="text-[var(--text-dim)] text-xs font-mono">{insight.hypothesis_id}</span>
                                  </div>
                                  <p className="text-[var(--text-primary)] text-sm">{insight.insight}</p>
                                  {insight.type && explanations[insight.type] && (
                                    <p className="text-[var(--text-dim)] text-xs mt-2">{explanations[insight.type]}</p>
                                  )}
                                </div>
                                <div className={`px-2 py-1 rounded text-xs font-bold ${confidenceColors[getConfidenceLevel(insight.confidence)]}`}>
                                  {Math.round(insight.confidence * 100)}%
                                </div>
                              </div>
                            )
                          })}
                        </div>
                      </div>
                    )}
                    
                    {analysis?.plots && analysis.plots.length > 0 && (
                      <div>
                        <p className="text-[var(--text-muted)] text-xs mb-2 flex items-center gap-2">
                          <TrendingUp className="w-3 h-3" />
                          Visualizations
                        </p>
                        <div className="grid grid-cols-2 gap-3">
                          {analysis.plots.map((plot: any, idx: number) => (
                            <div 
                              key={plot.plot_id || idx}
                              className="rounded-lg bg-[var(--bg-primary)] border border-[var(--border-subtle)] overflow-hidden"
                            >
                              {plot.data ? (
                                <img 
                                  src={`data:image/png;base64,${plot.data}`} 
                                  alt={plot.title}
                                  className="w-full h-32 object-cover"
                                />
                              ) : (
                                <div className="h-32 flex items-center justify-center bg-[var(--bg-secondary)]">
                                  <span className="text-[var(--text-dim)] text-xs">No data</span>
                                </div>
                              )}
                              <div className="p-2">
                                <p className="text-[var(--text-secondary)] text-xs truncate">{plot.title}</p>
                                <p className="text-[var(--text-dim)] text-xs">{plot.type}</p>
                              </div>
                            </div>
                          ))}
                        </div>
                      </div>
                    )}
                    
                    {analysis?.evidence && analysis.evidence.length > 0 && (
                      <div>
                        <p className="text-[var(--text-muted)] text-xs mb-2 flex items-center gap-2">
                          <Database className="w-3 h-3" />
                          Execution Trace
                        </p>
                        <div className="space-y-1">
                          {analysis.evidence.map((step: any, idx: number) => (
                            <div 
                              key={idx}
                              className="flex items-center gap-2 p-2 rounded bg-[var(--bg-primary)] text-xs"
                            >
                              {step.status === 'completed' ? (
                                <CheckCircle2 className="w-3 h-3 text-emerald-400" />
                              ) : step.status === 'failed' ? (
                                <XCircle className="w-3 h-3 text-red-400" />
                              ) : (
                                <Clock className="w-3 h-3 text-amber-400" />
                              )}
                              <span className="text-[var(--text-secondary)]">{step.phase}</span>
                              <span className="text-[var(--text-dim)]">•</span>
                              <span className="text-[var(--text-muted)]">
                                {Object.entries(step)
                                  .filter(([k]) => k !== 'phase' && k !== 'status')
                                  .map(([k, v]) => `${k}: ${typeof v === 'number' ? v.toLocaleString() : v}`)
                                  .join(', ')}
                              </span>
                            </div>
                          ))}
                        </div>
                      </div>
                    )}
                  </div>
                )}
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  )
}

export default MessageBubble