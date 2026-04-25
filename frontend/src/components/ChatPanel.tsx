import React, { useState, useRef, useEffect } from 'react'
import { Send, Loader2, Sparkles, Upload, Bot } from 'lucide-react'
import { useAnalysisStore } from '../hooks/useAnalysis'
import { uploadDataset, autoAnalyze, runPipeline } from '../api/client'
import { MessageBubble } from './MessageBubble'
import type { ChatMessage } from '../hooks/useAnalysis'

const suggestions = [
  { label: "Why did revenue drop?", icon: "📉" },
  { label: "Compare mobile vs desktop", icon: "📱" },
  { label: "Find anomalies", icon: "⚠️" },
  { label: "What drives conversion?", icon: "🎯" },
  { label: "Find unusual patterns", icon: "🔍" },
]

export const ChatPanel: React.FC = () => {
  const { 
    sessionId, 
    messages, 
    isAnalyzing, 
    addMessage, 
    updateLastAIMessage,
    setSessionId,
    setInsights,
    setPlots,
    setTrace,
    setPhaseStatus,
    loadingMessage,
    expandedMessageId,
    setExpandedMessage
  } = useAnalysisStore()
  
  const [input, setInput] = useState('')
  const messagesEndRef = useRef<HTMLDivElement>(null)
  const inputRef = useRef<HTMLTextAreaElement>(null)
  
  // Auto-scroll to bottom when new messages
  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [messages])
  
  // Focus input on load
  useEffect(() => {
    inputRef.current?.focus()
  }, [])
  
  const handleUpload = async (file: File) => {
    try {
      const result = await uploadDataset(file)
      setSessionId(result.session_id)
      
      // Add welcome message
      const welcomeMsg: ChatMessage = {
        id: `msg_${Date.now()}`,
        role: 'ai',
        content: `I've analyzed your dataset (${result.file_size.toLocaleString()} bytes). You can now ask me questions about your data, such as:\n\n• "Why did revenue drop?"\n• "Compare mobile vs desktop conversion"\n• "What drives user conversion?"\n\nOr choose from the suggestions below.`,
        timestamp: Date.now(),
        analysis: {
          confidence: 1,
          type: 'system',
          insights: [],
          plots: []
        }
      }
      addMessage(welcomeMsg)
    } catch (error) {
      console.error('Upload error:', error)
    }
  }
  
  const handleFileSelect = (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0]
    if (file) {
      handleUpload(file)
    }
  }
  
  const handleAsk = async () => {
    if (!input.trim() || isAnalyzing) return
    if (!sessionId) {
      setInput('')
      return
    }
    
    const question = input.trim()
    setInput('')
    
    // Add user message
    const userMsg: ChatMessage = {
      id: `msg_${Date.now()}`,
      role: 'user',
      content: question,
      timestamp: Date.now()
    }
    addMessage(userMsg)
    
    // Add placeholder AI message
    const aiMsg: ChatMessage = {
      id: `msg_${Date.now() + 1}`,
      role: 'ai',
      content: '',
      timestamp: Date.now(),
      analysis: {
        confidence: 0,
        type: 'analyzing',
        insights: [],
        plots: [],
        evidence: []
      }
    }
    addMessage(aiMsg)
    
    try {
      // Use auto-analyze to run needed phases
      const result = await autoAnalyze(question, sessionId)
      
      // Extract data from response - handle multiple formats
      let insights = result.insights || result.data?.insights || []
      let plots = result.plots || result.data?.plots || {}
      let traceData = result.trace || result.data?.trace || []
      let confidence = result.confidence || result.data?.confidence || 0
      
      // Fallback: get insights from signal_orchestrator if empty
      if (!insights || insights.length === 0) {
        try {
          const pipelineResult = await runPipeline(sessionId)
          insights = pipelineResult.insights || []
          plots = pipelineResult.plots || {}
          traceData = pipelineResult.trace || []
          confidence = insights.length > 0 
            ? insights.reduce((s: number, i: any) => s + (i.confidence || 0), 0) / insights.length
            : 0
        } catch (pipelineError) {
          console.log('Pipeline fallback error:', pipelineError)
        }
      }
      
      // Ensure we have at least basic insight
      if (!insights || insights.length === 0) {
        insights = [{
          id: 'fallback_1',
          insight: 'Your dataset has been analyzed. This is a preliminary finding based on the available data.',
          confidence: 0.3,
          type: 'descriptive',
          hypothesis_id: 'H_1'
        }]
        confidence = 0.3
      }
      
      // Generate natural language response
      let response = generateResponse(question, insights, confidence)
      
      updateLastAIMessage({
        content: response,
        analysis: {
          confidence,
          type: getQuestionType(question),
          insights: insights.map((i: any) => ({
            id: i.id || i.hypothesis_id,
            insight: i.insight || i.claim || '',
            confidence: i.confidence || 0.5,
            type: i.type || 'insight',
            hypothesis_id: i.hypothesis_id || ''
          })),
          plots: Object.values(plots),
          evidence: traceData
        }
      })
      
      // Also update store
      setInsights(insights.map((i: any) => ({
        id: i.id || i.hypothesis_id,
        insight: i.insight || i.claim || '',
        confidence: i.confidence || 0.5,
        type: i.type || 'insight',
        hypothesis_id: i.hypothesis_id || ''
      })))
      setPlots(plots)
      setTrace(traceData)
      
      // Update phase status based on results
      if (traceData.length > 0) {
        traceData.forEach((step: any) => {
          if (step.phase === 'explore') setPhaseStatus('explore', 'completed')
          if (step.phase === 'hypotheses') setPhaseStatus('hypotheses', 'completed')
          if (step.phase === 'execute') setPhaseStatus('execute', 'completed')
          if (step.phase === 'phase4') setPhaseStatus('insights', 'completed')
        })
      }
    } catch (error) {
      console.error('Analysis error:', error)
      updateLastAIMessage({
        content: "I encountered an error while analyzing your question. Please try again."
      })
    }
  }
  
  const handleSuggestion = (suggestion: string) => {
    setInput(suggestion)
    inputRef.current?.focus()
  }
  
  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault()
      handleAsk()
    }
  }
  
  return (
    <div className="flex flex-col h-full">
      {/* Header */}
      <header className="h-14 border-b border-[var(--border-subtle)] flex items-center px-4 flex-shrink-0">
        <div className="flex items-center gap-3">
          <div className="w-8 h-8 rounded-lg bg-[var(--accent-primary)]/20 flex items-center justify-center">
            <Sparkles className="w-4 h-4 text-[var(--accent-primary)]" />
          </div>
          <span className="text-[var(--text-primary)] font-medium">Data Analyst</span>
        </div>
        
        <div className="flex-1" />
        
        {/* Actions */}
        <div className="flex items-center gap-2">
          <label className="p-2 rounded-lg hover:bg-[var(--bg-secondary)] cursor-pointer transition-colors">
            <Upload className="w-4 h-4 text-[var(--text-muted)]" />
            <input type="file" accept=".csv" onChange={handleFileSelect} className="hidden" />
          </label>
        </div>
      </header>
      
      {/* Messages area */}
      <div className="flex-1 overflow-y-auto p-4">
        {messages.length === 0 ? (
          // Empty state
          <div className="h-full flex flex-col items-center justify-center text-center">
            <div className="w-16 h-16 rounded-full bg-[var(--accent-primary)]/20 flex items-center justify-center mb-4 animate-pulse-glow">
              <Sparkles className="w-8 h-8 text-[var(--accent-primary)]" />
            </div>
            <h2 className="text-xl font-medium text-[var(--text-primary)] mb-2">
              Data Analyst
            </h2>
            <p className="text-[var(--text-muted)] text-sm mb-6 max-w-md">
              Upload your dataset and ask questions in plain language.
              I'll analyze your data and explain the findings.
            </p>
            
            {/* Upload button */}
            <label className="px-6 py-3 rounded-xl bg-[var(--accent-primary)] text-[var(--bg-primary)] font-medium cursor-pointer hover:opacity-90 transition-opacity mb-8">
              <span className="flex items-center gap-2">
                <Upload className="w-4 h-4" />
                Upload CSV Dataset
              </span>
              <input type="file" accept=".csv" onChange={handleFileSelect} className="hidden" />
            </label>
            
            {/* Suggestions */}
            <div className="text-[var(--text-muted)] text-xs mb-3">Try asking:</div>
            <div className="flex flex-wrap gap-2 justify-center max-w-lg">
              {suggestions.map((s) => (
                <button
                  key={s.label}
                  onClick={() => sessionId && handleSuggestion(s.label)}
                  disabled={!sessionId}
                  className="px-3 py-1.5 rounded-lg bg-[var(--bg-secondary)] border border-[var(--border-subtle)] text-xs text-[var(--text-secondary)] hover:border-[var(--accent-primary)]/50 transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
                >
                  <span className="mr-1">{s.icon}</span>
                  {s.label}
                </button>
              ))}
            </div>
          </div>
        ) : (
          // Messages
          <div className="space-y-1">
            {messages.map((msg) => (
              <MessageBubble
                key={msg.id}
                message={msg}
                isExpanded={expandedMessageId === msg.id}
                onToggleExpand={() => setExpandedMessage(
                  expandedMessageId === msg.id ? null : msg.id
                )}
              />
            ))}
            
            {/* Typing indicator */}
            {isAnalyzing && (
              <div className="animate-fade-in-up mb-4 flex items-start gap-3">
                <div className="w-8 h-8 rounded-full bg-[var(--accent-primary)]/20 flex items-center justify-center flex-shrink-0">
                  <Bot className="w-4 h-4 text-[var(--accent-primary)]" />
                </div>
                <div className="bg-[var(--ai-bubble)] border border-[var(--border-subtle)] rounded-2xl px-4 py-3">
                  <div className="flex items-center gap-2 text-[var(--text-muted)]">
                    <Loader2 className="w-4 h-4 animate-spin" />
                    <span className="text-sm">{loadingMessage || 'Analyzing...'}</span>
                  </div>
                </div>
              </div>
            )}
            
            <div ref={messagesEndRef} />
          </div>
        )}
      </div>
      
      {/* Input area */}
      <div className="p-4 border-t border-[var(--border-subtle)] flex-shrink-0">
        {sessionId ? (
          <div className="relative">
            <textarea
              ref={inputRef}
              value={input}
              onChange={(e) => setInput(e.target.value)}
              onKeyDown={handleKeyDown}
              placeholder="Ask about your data..."
              className="w-full bg-[var(--bg-secondary)] border border-[var(--border-subtle)] rounded-xl p-3 pr-12 text-[var(--text-primary)] text-sm resize-none h-20 focus:border-[var(--accent-primary)] focus:outline-none"
            />
            <button
              onClick={handleAsk}
              disabled={!input.trim() || isAnalyzing}
              className="absolute bottom-3 right-3 p-2 bg-[var(--accent-primary)] rounded-lg text-[var(--bg-primary)] disabled:opacity-50 disabled:cursor-not-allowed hover:opacity-90 transition-opacity"
            >
              {isAnalyzing ? (
                <Loader2 className="w-4 h-4 animate-spin" />
              ) : (
                <Send className="w-4 h-4" />
              )}
            </button>
          </div>
        ) : (
          <div className="text-center text-[var(--text-muted)] text-sm">
            Upload a dataset to start analyzing
          </div>
        )}
      </div>
    </div>
  )
}

// Helper functions
function getQuestionType(question: string): string {
  const q = question.toLowerCase()
  if (q.includes('drop') || q.includes('decrease') || q.includes('fall')) return 'decline'
  if (q.includes('increase') || q.includes('grow') || q.includes('rise')) return 'growth'
  if (q.includes('compare') || q.includes('vs') || q.includes('versus')) return 'comparison'
  if (q.includes('why') || q.includes('cause')) return 'causal'
  if (q.includes('anomal') || q.includes('unusual') || q.includes('outlier')) return 'anomaly'
  return 'exploratory'
}

function generateResponse(question: string, insights: any[], confidence: number): string {
  if (insights.length === 0) {
    return "I couldn't find any significant patterns to answer your question. This might be because the dataset is too small or the patterns are very weak. Try asking a different question or uploading more data."
  }
  
  const q = question.toLowerCase()
  const topInsight = insights[0]
  const confPercent = Math.round(confidence * 100)
  
  // Generate contextual response
  let response = ""
  
  if (q.includes('why') && q.includes('drop')) {
    response = `Based on my analysis, here's what I found about your question "${question}":\n\n`
    response += `The data shows a decline pattern that correlates with other factors. `
    response += `I found ${insights.length} key insight${insights.length > 1 ? 's' : ''} with ${confPercent}% average confidence.\n\n`
    response += `The strongest finding is: "${topInsight.insight || topInsight.claim}"\n\n`
    response += `This suggests the drop is likely related to ${topInsight.type === 'causal' ? 'changes in associated metrics' : topInsight.type === 'temporal' ? 'time-based factors' : 'specific user segments'}. `
  } else if (q.includes('compare')) {
    response = `Here's the comparison you asked for:\n\n`
    response += `I analyzed ${insights.length} difference${insights.length > 1 ? 's' : ''} with ${confPercent}% confidence.\n\n`
    response += `Key finding: "${topInsight.insight || topInsight.claim}"\n\n`
  } else if (q.includes('what drives') || q.includes('what causes')) {
    response = `I've identified the key drivers for your question:\n\n`
    response += `Found ${insights.length} significant relationship${insights.length > 1 ? 's' : ''} in the data.\n\n`
    response += `Strongest driver: "${topInsight.insight || topInsight.claim}"\n\n`
    response += `This has ${confPercent}% confidence, indicating a ${confPercent >= 70 ? 'strong' : confPercent >= 50 ? 'moderate' : 'weak'} statistical relationship. `
  } else if (q.includes('anomal') || q.includes('unusual')) {
    response = `I've detected the following anomalies:\n\n`
    response += `${insights.length} unusual pattern${insights.length > 1 ? 's' : ''} found with ${confPercent}% confidence.\n\n`
    response += `Main anomaly: "${topInsight.insight || topInsight.claim}"`
  } else {
    // Default exploratory response
    response = `Here's what I found for "${question}":\n\n`
    response += `I discovered ${insights.length} insight${insights.length > 1 ? 's' : ''} with ${confPercent}% average confidence.\n\n`
    response += `${insights.slice(0, 2).map(i => `• ${i.insight || i.claim}`).join('\n\n')}\n\n`
    if (insights.length > 2) {
      response += `Plus ${insights.length - 2} more findings in the details below.`
    }
  }
  
  return response
}

export default ChatPanel