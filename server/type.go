package server

import "net/http"

type responseWriter struct { 
	http.ResponseWriter
	statusCode int
	bytesWritten int
}

func (rw *responseWriter) WriteHeader(statusCode int) { 
	rw.statusCode = statusCode
	rw.ResponseWriter.WriteHeader(statusCode)
}

func (rw *responseWriter) Write(b []byte) (int, error) { 
	if rw.statusCode == 0 { 
		rw.statusCode = http.StatusOK
	}

	n, err := rw.ResponseWriter.Write(b)
	rw.bytesWritten += n
	return n, err
}