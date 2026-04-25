declare module 'react-plotly.js' {
  import { Component } from 'react'
  import Plotly from 'plotly.js'
  
  interface PlotParams {
    data?: Plotly.Data[]
    layout?: Plotly.Layout
    frames?: Plotly.Frame[]
    config?: Plotly.Config
  }
  
  class Plot extends Component<PlotParams> {
    static plotly: typeof Plotly
  }
  
  export default Plot
}