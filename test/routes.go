func (c *RouterConfig) InitRouter() *gin.Engine {
	if gin.Mode() != gin.TestMode {
		c.R.Use(gin.Recovery())
		c.R.Use(gin.LoggerWithFormatter(func(param gin.LogFormatterParams) string {
			return fmt.Sprintf("[%s] \"%s - %s %s %s %d %s \"%s\" %s\"",
				param.TimeStamp.Format(time.RFC1123),
				param.ClientIP,
				param.Method,
				param.Path,
				param.Request.Proto,
				param.StatusCode,
				param.Latency,
				param.Request.UserAgent(),
				param.ErrorMessage,
			)
		}))
	}
	// Cors Configuration
	cors, err := fcors.AllowAccess(
		fcors.FromAnyOrigin(),
		fcors.WithMethods(
			http.MethodGet,
			http.MethodPost,
			http.MethodPut,
			http.MethodDelete,
		),
	)
	if err != nil {
		c.Logger.Fatalf("[routes] Failed to setup cors: %v", err)
		os.Exit(1)
	}
	// apply the CORS middleware to the engine
	c.R.Use(adapter.Wrap(cors))
	c.R.Use(c.secureMiddleware())
	c.customRoutes()
	c.Interceptor = middlewares.NewInterceptor(c.Logger)
	return c.R
}