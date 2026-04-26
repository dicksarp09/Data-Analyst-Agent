import React from 'react'
import { useDropzone } from 'react-dropzone'
import { Upload, Loader2 } from 'lucide-react'
import { useAnalysisStore } from '../hooks/useAnalysis'
import { uploadDataset, runPipeline } from '../api/client'
import { pollFirstScreen, subscribeToProgress } from '../hooks/useAnalysis'

export const UploadZone: React.FC = () => {
  const { setSessionId, setDatasetMeta, setInsights, setPlots, setTrace, setLoading, setPhaseStatus, isLoading, loadingMessage } = useAnalysisStore()
  
  const { getRootProps, getInputProps, isDragActive } = useDropzone({
    accept: { 'text/csv': ['.csv'] },
    maxFiles: 1,
    onDrop: async (acceptedFiles) => {
      const file = acceptedFiles[0]
      if (!file) return
      
      setLoading(true, 'Uploading dataset...')
      
      try {
        // Upload
        const uploadResult = await uploadDataset(file)
        const sessionId = uploadResult.session_id
        setSessionId(sessionId)
        
        setLoading(true, 'Waiting for analysis to complete...')

        // Subscribe to progress SSE and poll first_screen until ready
        const es = subscribeToProgress(sessionId, (progress) => {
          if (progress && progress.phase) {
            // map incoming progress to phase status
            try {
              setPhaseStatus(progress.phase, progress.status || 'running')
            } catch (e) {
              // ignore
            }
          }
          setLoading(true, progress?.message || 'Processing...')
        })

        try {
          const firstScreen = await pollFirstScreen(sessionId, (p) => {
            setLoading(true, p?.message || 'Processing...')
          })

          // Update state from first screen payload
          setDatasetMeta({
            rows: firstScreen.dataset_meta?.rows || 0,
            columns: firstScreen.dataset_meta?.columns || 0,
            quality: firstScreen.dataset_meta?.quality || {}
          })

          const plotsRecord: Record<string, any> = {}
          if (firstScreen.plots) {
            // firstScreen.plots may be array or object
            if (Array.isArray(firstScreen.plots)) {
              firstScreen.plots.forEach((p: any) => { plotsRecord[p.plot_id || p.id || p.title] = p })
            } else {
              Object.entries(firstScreen.plots).forEach(([key, plot]) => { plotsRecord[key] = plot })
            }
          }

          setPlots(plotsRecord)
          setInsights(firstScreen.insights || [])
          setTrace(firstScreen.trace || [])
        } finally {
          try { es.close() } catch (e) {}
          setLoading(false)
        }
      } catch (error) {
        console.error('Upload failed:', error)
        setLoading(false)
      }
    }
  })
  
  return (
    <div
      {...getRootProps()}
      className={`
        border-2 border-dashed rounded-lg p-8 text-center cursor-pointer transition-all
        ${isDragActive ? 'border-blue-500 bg-blue-500/10' : 'border-gray-600 hover:border-gray-500'}
        ${isLoading ? 'opacity-50 pointer-events-none' : ''}
      `}
    >
      <input {...getInputProps()} />
      
      {isLoading ? (
        <div className="flex flex-col items-center gap-3">
          <Loader2 className="w-10 h-10 animate-spin text-blue-400" />
          <p className="text-gray-300">{loadingMessage || 'Processing...'}</p>
        </div>
      ) : (
        <div className="flex flex-col items-center gap-3">
          <Upload className="w-10 h-10 text-gray-400" />
          <div>
            <p className="text-gray-200 font-medium">
              {isDragActive ? 'Drop CSV here' : 'Upload dataset'}
            </p>
            <p className="text-gray-500 text-sm">or drag and drop CSV file</p>
          </div>
        </div>
      )}
    </div>
  )
}