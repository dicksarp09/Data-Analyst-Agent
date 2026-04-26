import axios from 'axios'

const API_BASE = '/'

export const uploadDataset = async (file: File): Promise<any> => {
  const formData = new FormData()
  formData.append('file', file)
  
  const response = await axios.post(`${API_BASE}upload_csv`, formData)
  return response.data
}

export const getFirstScreen = async (sessionId: string) => {
  const response = await axios.get(`${API_BASE}first_screen/${sessionId}`)
  return response.data
}

export const runPipeline = async (sessionId: string) => {
  const response = await axios.post(`${API_BASE}run_pipeline`, {
    session_id: sessionId
  })
  return response.data
}

// Chat with NLQ
export const askQuestion = async (question: string, sessionId: string) => {
  const response = await axios.post(`${API_BASE}nlq/ask`, {
    question,
    session_id: sessionId
  })
  return response.data
}

// Auto-analyze (runs needed phases automatically)
export const autoAnalyze = async (question: string, sessionId: string) => {
  const response = await axios.post(`${API_BASE}auto_analyze`, {
    query: question,
    session_id: sessionId
  })
  return response.data
}

// Get schema
export const getSchema = async (sessionId: string) => {
  const response = await axios.get(`${API_BASE}schema/${sessionId}`)
  return response.data
}

// Get data quality
export const getDataQuality = async (sessionId: string) => {
  const response = await axios.get(`${API_BASE}data_quality/${sessionId}`)
  return response.data
}

// Get signals
export const getSignals = async (sessionId: string) => {
  const response = await axios.get(`${API_BASE}signals/${sessionId}`)
  return response.data
}

// Get hypotheses
export const getHypotheses = async (sessionId: string) => {
  const response = await axios.get(`${API_BASE}hypotheses/${sessionId}`)
  return response.data
}

// Get execution results
export const getExecution = async (sessionId: string) => {
  const response = await axios.get(`${API_BASE}execution/${sessionId}`)
  return response.data
}

// Get phase4 insights
export const getPhase4Insights = async (sessionId: string) => {
  const response = await axios.get(`${API_BASE}phase4/insights/${sessionId}`)
  return response.data
}

// Get phase4 plots
export const getPhase4Plots = async (sessionId: string) => {
  const response = await axios.get(`${API_BASE}phase4/plots/${sessionId}`)
  return response.data
}

export const approveExecution = async (sessionId: string) => {
  const response = await axios.post(`${API_BASE}execution/${sessionId}/approve`)
  return response.data
}

export const rejectExecution = async (sessionId: string, reason?: string) => {
  const response = await axios.post(`${API_BASE}execution/${sessionId}/reject`, { reason })
  return response.data
}

// Natural language query
export const nlqAsk = async (question: string, sessionId: string) => {
  const response = await axios.post(`${API_BASE}nlq/ask`, {
    question,
    session_id: sessionId
  })
  return response.data
}

// Chat with insights
export const chatAsk = async (question: string, sessionId: string) => {
  const response = await axios.post(`${API_BASE}chat`, {
    query: question,
    session_id: sessionId
  })
  return response.data
}

// Run simulation
export const runSimulation = async (sessionId: string, variable: string, changePercent: number) => {
  const response = await axios.post(`${API_BASE}simulate`, {
    session_id: sessionId,
    variable: variable,
    change_percent: changePercent
  })
  return response.data
}

// Get report
export const getReport = async (sessionId: string, format: 'json' | 'html' = 'json') => {
  const response = await axios.get(`${API_BASE}report/${sessionId}?format=${format}`)
  return response.data
}

// Progress stream (SSE)
export const subscribeToProgress = (sessionId: string) => {
  return new EventSource(`${API_BASE}progress/stream/${sessionId}`)
}