package runtime

import (
	"fmt"
	"io"
	"net/http"
	"net/textproto"

	"github.com/golang/protobuf/proto"
	"github.com/grpc-ecosystem/grpc-gateway/runtime/internal"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/grpclog"
	"google.golang.org/grpc/status"
)

type StreamHandlerFunc func(context.Context, *ServeMux, Marshaler, http.ResponseWriter, *http.Request, bool, interface{}) error

var _ StreamHandlerFunc = defaultStreamHandler

func defaultStreamHandler(_ context.Context, _ *ServeMux, marshaler Marshaler, w http.ResponseWriter, _ *http.Request, first bool, resp interface{}) error {
	msg := make(map[string]proto.Message, 1)
	switch v := resp.(type) {
	case error:
		grpcCode := codes.Unknown
		if s, ok := status.FromError(v); ok {
			grpcCode = s.Code()
		}
		httpCode := HTTPStatusFromCode(grpcCode)
		if first {
			w.WriteHeader(httpCode)
		}
		msg["error"] = &internal.StreamError{
			GrpcCode:   int32(grpcCode),
			HttpCode:   int32(httpCode),
			Message:    v.Error(),
			HttpStatus: http.StatusText(httpCode),
		}
	case proto.Message:
		msg["result"] = v
	}
	buf, err := marshaler.Marshal(msg)
	if err != nil {
		// TODO: Handle writing a message if marshaling fails.
		// Have a fallback if the marshaling error cannot be marshaled.
		return err
	}
	_, err = w.Write(buf)
	return err
}

// ForwardResponseStream forwards the stream from gRPC server to REST client.
func ForwardResponseStream(ctx context.Context, mux *ServeMux, marshaler Marshaler, w http.ResponseWriter, req *http.Request, recv func() (proto.Message, error), opts ...func(context.Context, http.ResponseWriter, proto.Message) error) {
	w.Header().Set("Content-Type", marshaler.ContentType())
	w.Header().Set("Transfer-Encoding", "chunked")

	f, ok := w.(http.Flusher)
	if !ok {
		grpclog.Printf("Flush not supported in %T", w)
		mux.streamHandler(ctx, mux, marshaler, w, req, true, status.Error(codes.Internal, "unexpected type of web server"))
		return
	}

	md, ok := ServerMetadataFromContext(ctx)
	if !ok {
		grpclog.Printf("Failed to extract ServerMetadata from context")
		mux.streamHandler(ctx, mux, marshaler, w, req, true, status.Error(codes.Internal, "unexpected error"))
		return
	}
	handleForwardResponseServerMetadata(w, mux, md)

	if err := handleForwardResponseOptions(ctx, w, nil, opts); err != nil {
		mux.streamHandler(ctx, mux, marshaler, w, req, true, err)
		return
	}

	var delimiter []byte
	if d, ok := marshaler.(Delimited); ok {
		delimiter = d.Delimiter()
	} else {
		delimiter = []byte("\n")
	}

	for first := true; ; first = false {
		resp, err := recv()
		if err == io.EOF {
			return
		}
		if err != nil {
			mux.streamHandler(ctx, mux, marshaler, w, req, first, err)
			return
		}
		if resp == nil {
			mux.streamHandler(ctx, mux, marshaler, w, req, first, status.Error(codes.Internal, "empty response"))
			return
		}
		if err := handleForwardResponseOptions(ctx, w, resp, opts); err != nil {
			mux.streamHandler(ctx, mux, marshaler, w, req, first, err)
			return
		}

		if err := mux.streamHandler(ctx, mux, marshaler, w, req, first, resp); err != nil {
			grpclog.Printf("Failed to send response chunk: %v", err)
			return
		}
		if _, err = w.Write(delimiter); err != nil {
			grpclog.Printf("Failed to send delimiter chunk: %v", err)
			return
		}
		f.Flush()
	}
}

func handleForwardResponseServerMetadata(w http.ResponseWriter, mux *ServeMux, md ServerMetadata) {
	for k, vs := range md.HeaderMD {
		if h, ok := mux.outgoingHeaderMatcher(k); ok {
			for _, v := range vs {
				w.Header().Add(h, v)
			}
		}
	}
}

func handleForwardResponseTrailerHeader(w http.ResponseWriter, md ServerMetadata) {
	for k := range md.TrailerMD {
		tKey := textproto.CanonicalMIMEHeaderKey(fmt.Sprintf("%s%s", MetadataTrailerPrefix, k))
		w.Header().Add("Trailer", tKey)
	}
}

func handleForwardResponseTrailer(w http.ResponseWriter, md ServerMetadata) {
	for k, vs := range md.TrailerMD {
		tKey := fmt.Sprintf("%s%s", MetadataTrailerPrefix, k)
		for _, v := range vs {
			w.Header().Add(tKey, v)
		}
	}
}

// ForwardResponseMessage forwards the message "resp" from gRPC server to REST client.
func ForwardResponseMessage(ctx context.Context, mux *ServeMux, marshaler Marshaler, w http.ResponseWriter, req *http.Request, resp proto.Message, opts ...func(context.Context, http.ResponseWriter, proto.Message) error) {
	md, ok := ServerMetadataFromContext(ctx)
	if !ok {
		grpclog.Printf("Failed to extract ServerMetadata from context")
	}

	handleForwardResponseServerMetadata(w, mux, md)
	handleForwardResponseTrailerHeader(w, md)
	w.Header().Set("Content-Type", marshaler.ContentType())
	if err := handleForwardResponseOptions(ctx, w, resp, opts); err != nil {
		HTTPError(ctx, mux, marshaler, w, req, err)
		return
	}

	buf, err := marshaler.Marshal(resp)
	if err != nil {
		grpclog.Printf("Marshal error: %v", err)
		HTTPError(ctx, mux, marshaler, w, req, err)
		return
	}

	if _, err = w.Write(buf); err != nil {
		grpclog.Printf("Failed to write response: %v", err)
	}

	handleForwardResponseTrailer(w, md)
}

func handleForwardResponseOptions(ctx context.Context, w http.ResponseWriter, resp proto.Message, opts []func(context.Context, http.ResponseWriter, proto.Message) error) error {
	if len(opts) == 0 {
		return nil
	}
	for _, opt := range opts {
		if err := opt(ctx, w, resp); err != nil {
			grpclog.Printf("Error handling ForwardResponseOptions: %v", err)
			return err
		}
	}
	return nil
}
