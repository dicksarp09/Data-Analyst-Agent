import { create } from 'zustand'

export type ViewMode = 'chat' | 'insights' | 'report' | 'explore' | 'insight' | 'simulation'

export interface Insight {
  id: string
  insight: string
  confidence: number
  type: string
  hypothesis_id: string
  plot_ids: string[]
  effect_size?: number
  claim?: string
}

export interface Plot {
  plot_id: string
  title: string
  type: string
  data?: string
  x?: string[]
  y?: number[]
}

export interface DatasetMeta {
  rows: number
  columns: number
  quality: Record<string, any>
}

export interface TraceStep {
  phase: string
  [key: string]: any
}

export interface ChatMessage {
  id: string
  role: 'user' | 'ai'
  content: string
  timestamp: number
  analysis?: {
    confidence: number
    type: string
    insights?: Insight[]
    plots?: Plot[]
    evidence?: TraceStep[]
  }
}

export interface PhaseStatus {
  explore: 'pending' | 'running' | 'completed' | 'failed'
  hypotheses: 'pending' | 'running' | 'completed' | 'failed'
  execute: 'pending' | 'running' | 'completed' | 'failed'
  insights: 'pending' | 'running' | 'completed' | 'failed'
}

interface AppState {
  // Core data
  sessionId: string | null
  datasetMeta: DatasetMeta | null
  
  // Chat
  messages: ChatMessage[]
  isAnalyzing: boolean
  
  // Analysis results
  insights: Insight[]
  plots: Record<string, Plot>
  
  // UI state (backward compatibility)
  selectedInsightId: string | null
  viewMode: ViewMode
  expandedMessageId: string | null
  
  // Trace
  trace: TraceStep[]
  phaseStatus: PhaseStatus
  
  // Loading (backward compatibility)
  isLoading: boolean
  loadingMessage: string
  
  // Actions
  setSessionId: (id: string | null) => void
  setDatasetMeta: (meta: DatasetMeta | null) => void
  
  addMessage: (message: ChatMessage) => void
  updateLastAIMessage: (updates: Partial<ChatMessage>) => void
  clearMessages: () => void
  
  setInsights: (insights: Insight[]) => void
  setPlots: (plots: Record<string, Plot>) => void
  selectInsight: (id: string | null) => void
  setViewMode: (mode: ViewMode) => void
  setExpandedMessage: (id: string | null) => void
  
  setTrace: (trace: TraceStep[]) => void
  setPhaseStatus: (phase: keyof PhaseStatus, status: PhaseStatus[keyof PhaseStatus]) => void
  
  setAnalyzing: (analyzing: boolean, message?: string) => void
  setLoading: (loading: boolean, message?: string) => void
  
  // Computed (backward compatibility)
  getSelectedInsight: () => Insight | undefined
  getInsightPlots: () => Plot[]
  getSelectedInsights: () => Insight[]
  getMessageInsights: (messageId: string) => Insight[]
}

export const useAnalysisStore = create<AppState>((set, get) => ({
  // Initial state
  sessionId: null,
  datasetMeta: null,
  
  messages: [],
  isAnalyzing: false,
  
  insights: [],
  plots: {},
  
  selectedInsightId: null,
  viewMode: 'chat',
  expandedMessageId: null,
  
  trace: [],
  phaseStatus: {
    explore: 'pending',
    hypotheses: 'pending',
    execute: 'pending',
    insights: 'pending'
  },
  
  isLoading: false,
  loadingMessage: '',
  
  // Actions
  setSessionId: (id) => set({ sessionId: id }),
  setDatasetMeta: (meta) => set({ datasetMeta: meta }),
  
  addMessage: (message) => set((state) => ({ 
    messages: [...state.messages, message] 
  })),
  
  updateLastAIMessage: (updates) => set((state) => {
    const messages = [...state.messages]
    const lastIndex = messages.length - 1
    if (lastIndex >= 0 && messages[lastIndex].role === 'ai') {
      messages[lastIndex] = { ...messages[lastIndex], ...updates }
    }
    return { messages }
  }),
  
  clearMessages: () => set({ messages: [] }),
  
  setInsights: (insights) => {
    const current = get()
    let selectedId = current.selectedInsightId
    
    if (insights.length > 0 && (!selectedId || !insights.find(i => i.id === selectedId))) {
      selectedId = insights[0].id
    }
    
    set({ 
      insights, 
      selectedInsightId: selectedId,
      viewMode: 'chat'
    })
  },
  
  setPlots: (plots) => set({ plots }),
  
  selectInsight: (id) => set({ 
    selectedInsightId: id, 
    viewMode: 'insight' 
  }),
  
  setViewMode: (mode) => set({ viewMode: mode }),
  setExpandedMessage: (id) => set({ expandedMessageId: id }),
  
  setTrace: (trace) => set({ trace }),
  setPhaseStatus: (phase, status) => set((state) => ({
    phaseStatus: { ...state.phaseStatus, [phase]: status }
  })),
  
  setAnalyzing: (analyzing, message = '') => set({ 
    isAnalyzing: analyzing, 
    loadingMessage: message 
  }),
  
  setLoading: (loading, message = '') => set({ 
    isLoading: loading, 
    loadingMessage: message 
  }),
  
  // Computed getters
  getSelectedInsight: () => {
    const { insights, selectedInsightId } = get()
    return insights.find(i => i.id === selectedInsightId)
  },
  
  getInsightPlots: () => {
    const { plots, insights, selectedInsightId } = get()
    const insight = insights.find(i => i.id === selectedInsightId)
    if (!insight) return []
    
    return insight.plot_ids
      .map(pid => plots[pid])
      .filter(Boolean)
  },
  
  getSelectedInsights: () => {
    const { insights } = get()
    return insights.sort((a, b) => b.confidence - a.confidence)
  },
  
  getMessageInsights: (messageId) => {
    const message = get().messages.find(m => m.id === messageId)
    return message?.analysis?.insights || []
  }
}))