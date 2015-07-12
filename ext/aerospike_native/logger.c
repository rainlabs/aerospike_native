#include "logger.h"
#include "client.h"

VALUE LoggerClass;

bool aerospike_log_callback(as_log_level level, const char *func, const char *file,
    uint32_t line, const char *fmt, ...)
{
    char msg[1024] = {0};
    char log_msg[5000] = {0};
    va_list ap;
    ID vMethod = rb_intern("info");
    VALUE vLogger = rb_cv_get(ClientClass, "@@logger");

    va_start(ap, fmt);
    vsnprintf(msg, 1024, fmt, ap);
    msg[1023] = '\0';
    va_end(ap);

    sprintf(log_msg, "%d (Aerospike) - %s\n", level, msg);

    if(TYPE(vLogger) != T_NIL) {
        VALUE vInteral;
        switch(level) {
        case AS_LOG_LEVEL_ERROR:
            vMethod = rb_intern("error");
            break;
        case AS_LOG_LEVEL_WARN:
            vMethod = rb_intern("warn");
            break;
        case AS_LOG_LEVEL_INFO:
            vMethod = rb_intern("info");
            break;
        case AS_LOG_LEVEL_DEBUG:
            vMethod = rb_intern("debug");
            break;
        case AS_LOG_LEVEL_TRACE:
            vMethod = rb_intern("trace");
            break;
        }

        vInteral = rb_iv_get(vLogger, "@internal");

        if (TYPE(vInteral) == T_NIL) {
            rb_funcall(vLogger, vMethod, 1, rb_str_new2(log_msg));
        } else {
            rb_funcall(vInteral, vMethod, 1, rb_str_new2(log_msg));
        }
    }

    return true;
}

VALUE logger_initialize(VALUE vSelf)
{
    as_log_set_level(AS_LOG_LEVEL_INFO);
    as_log_set_callback(aerospike_log_callback);
    rb_iv_set(vSelf, "@level", ID2SYM( rb_intern("info") ));
    return vSelf;
}

VALUE logger_set_level(VALUE vSelf, VALUE vLevel)
{
    int log_level = AS_LOG_LEVEL_INFO;
    VALUE vError = ID2SYM( rb_intern("error") );
    VALUE vWarn = ID2SYM( rb_intern("warn") );
    VALUE vInfo = ID2SYM( rb_intern("info") );
    VALUE vDebug = ID2SYM( rb_intern("debug") );
    VALUE vTrace = ID2SYM( rb_intern("trace") );

    Check_Type(vLevel, T_SYMBOL);
    if (vLevel == vError) {
        log_level = AS_LOG_LEVEL_ERROR;
    } else if (vLevel == vWarn) {
        log_level = AS_LOG_LEVEL_WARN;
    } else if (vLevel == vInfo) {
        log_level = AS_LOG_LEVEL_INFO;
    } else if (vLevel == vDebug) {
        log_level = AS_LOG_LEVEL_DEBUG;
    } else if (vLevel == vTrace) {
        log_level = AS_LOG_LEVEL_TRACE;
    } else {
        return Qfalse;
    }

    as_log_set_level(log_level);
    rb_iv_set(vSelf, "@level", vLevel);
    return Qtrue;
}

VALUE logger_write(VALUE vSelf, VALUE vMsg)
{
    Check_Type(vMsg, T_STRING);
    printf(StringValueCStr(vMsg));
    return Qnil;
}

void define_logger()
{
    LoggerClass = rb_define_class_under(AerospikeNativeClass, "Logger", rb_cObject);
    rb_define_method(LoggerClass, "initialize", logger_initialize, 0);
    rb_define_method(LoggerClass, "set_level", logger_set_level, 1);
    rb_define_method(LoggerClass, "error", logger_write, 1);
    rb_define_method(LoggerClass, "warn", logger_write, 1);
    rb_define_method(LoggerClass, "info", logger_write, 1);
    rb_define_method(LoggerClass, "debug", logger_write, 1);
    rb_define_method(LoggerClass, "trace", logger_write, 1);
    rb_define_attr(LoggerClass, "level", 1, 0);
    rb_define_attr(LoggerClass, "internal", 1, 1);
}
