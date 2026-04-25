import React from 'react'
import { ChatPanel } from './components/ChatPanel'

export const App: React.FC = () => {
  return (
    <div className="min-h-screen bg-[var(--bg-primary)]">
      <ChatPanel />
    </div>
  )
}

export default App