import React from 'react'
import { useDropzone } from 'react-dropzone'
import { Upload, Loader2 } from 'lucide-react'
import { useAnalysisStore } from '../hooks/useAnalysis'
import { uploadDataset, runPipeline } from '../api/client'

export const UploadZone: React.FC = () => {
  const { setSessionId, setDatasetMeta, setInsights, setPlots, setTrace, setLoading, isLoading, loadingMessage } = useAnalysisStore()
  
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
        
        setLoading(true, 'Running analysis pipeline...')
        
        // Run full pipeline
        const pipelineResult = await runPipeline(sessionId)
        
        // Update state
        setDatasetMeta({
          rows: pipelineResult.dataset_meta?.rows || 0,
          columns: pipelineResult.dataset_meta?.columns || 0,
          quality: pipelineResult.dataset_meta?.quality || {}
        })
        
        // Convert plots array to record
        const plotsRecord: Record<string, any> = {}
        if (pipelineResult.plots) {
          Object.entries(pipelineResult.plots).forEach(([key, plot]) => {
            plotsRecord[key] = plot
          })
        }
        
        setPlots(plotsRecord)
        setInsights(pipelineResult.insights || [])
        setTrace(pipelineResult.trace || [])
        
        setLoading(false)
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