#include "logger.h"
#include "client.h"

VALUE LoggerClass;

bool aerospike_log_callback(as_log_level level, const char *func, const char *file,
    uint32_t line, const char *fmt, ...)
{
    char msg[1024] = {0};
    va_list ap;

    va_start(ap, fmt);
    vsnprintf(msg, 1024, fmt, ap);
    msg[1023] = '\0';
    va_end(ap);

    if(TYPE(LoggerInstance) != T_NIL) {
        rb_funcall(LoggerInstance, rb_intern("write"), 2, INT2FIX(level), rb_str_new2(msg));
    }

    return true;
}

VALUE logger_initialize(VALUE vSelf)
{
    int default_level = AS_LOG_LEVEL_DEBUG;
    as_log_set_level(default_level);
    as_log_set_callback(aerospike_log_callback);
    rb_iv_set(vSelf, "@level", INT2FIX(default_level));
    return vSelf;
}

VALUE logger_set_level(VALUE vSelf, VALUE vLevel)
{
    VALUE vInternalLogger = rb_iv_get(vSelf, "@internal");
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
    rb_iv_set(vSelf, "@level", INT2FIX(log_level));

    if (TYPE(vInternalLogger) != T_NIL) {
        int internal_level = 3 - (log_level == AS_LOG_LEVEL_TRACE ? AS_LOG_LEVEL_DEBUG : log_level);
        rb_iv_set(vInternalLogger, "@level", INT2FIX(internal_level));
    }

    return Qtrue;
}

VALUE logger_write(VALUE vSelf, VALUE vLevel, VALUE vMsg)
{
    int level;
    VALUE vInternalLevel, vInternalLogger;
    ID vMethod;
    Check_Type(vLevel, T_FIXNUM);
    Check_Type(vMsg, T_STRING);

    vInternalLevel = rb_iv_get(vSelf, "@level");
    vInternalLogger = rb_iv_get(vSelf, "@internal");
    Check_Type(vInternalLevel, T_FIXNUM);
    level = FIX2INT(vLevel);

    switch(TYPE(vInternalLogger)) {
    case T_NIL:
        if (level <= FIX2INT(vInternalLevel)) {
            switch(level) {
            case AS_LOG_LEVEL_ERROR:
                vMsg = rb_str_plus(rb_str_new2("ERROR (AEROSPIKE): "), vMsg);
                break;
            case AS_LOG_LEVEL_WARN:
                vMsg = rb_str_plus(rb_str_new2("WARN  (AEROSPIKE): "), vMsg);
                break;
            case AS_LOG_LEVEL_INFO:
                vMsg = rb_str_plus(rb_str_new2("INFO  (AEROSPIKE): "), vMsg);
                break;
            case AS_LOG_LEVEL_DEBUG:
                vMsg = rb_str_plus(rb_str_new2("DEBUG (AEROSPIKE): "), vMsg);
                break;
            case AS_LOG_LEVEL_TRACE:
                vMsg = rb_str_plus(rb_str_new2("DEBUG (AEROSPIKE): "), vMsg);
                break;
            default:
                vMsg = rb_str_plus(rb_str_new2("UNKNOWN (AEROSPIKE): "), vMsg);
                break;
            }
            // TODO: add default behavior with write into file
            fprintf(stdout, "%s\n", StringValueCStr(vMsg));
            return vMsg;
        }
        break;
    case T_OBJECT:
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
        case AS_LOG_LEVEL_TRACE:
            vMethod = rb_intern("debug");
            break;
        default:
            vMethod = rb_intern("unknown");
            break;
        }

        return rb_funcall(vInternalLogger, vMethod, 1, vMsg);
    }

    return Qnil;
}

VALUE logger_error(VALUE vSelf, VALUE vMsg)
{
    return rb_funcall(vSelf, rb_intern("write"), 2, INT2FIX(AS_LOG_LEVEL_ERROR), vMsg);
}

VALUE logger_warn(VALUE vSelf, VALUE vMsg)
{
    return rb_funcall(vSelf, rb_intern("write"), 2, INT2FIX(AS_LOG_LEVEL_WARN), vMsg);
}

VALUE logger_info(VALUE vSelf, VALUE vMsg)
{
    return rb_funcall(vSelf, rb_intern("write"), 2, INT2FIX(AS_LOG_LEVEL_INFO), vMsg);
}

VALUE logger_debug(VALUE vSelf, VALUE vMsg)
{
    return rb_funcall(vSelf, rb_intern("write"), 2, INT2FIX(AS_LOG_LEVEL_DEBUG), vMsg);
}

VALUE logger_trace(VALUE vSelf, VALUE vMsg)
{
    return rb_funcall(vSelf, rb_intern("write"), 2, INT2FIX(AS_LOG_LEVEL_TRACE), vMsg);
}

void define_logger()
{
    LoggerClass = rb_define_class_under(AerospikeNativeClass, "Logger", rb_cObject);
    rb_define_method(LoggerClass, "initialize", logger_initialize, 0);
    rb_define_method(LoggerClass, "set_level", logger_set_level, 1);
    rb_define_method(LoggerClass, "write", logger_write, 2);
    rb_define_method(LoggerClass, "error", logger_error, 1);
    rb_define_method(LoggerClass, "warn", logger_warn, 1);
    rb_define_method(LoggerClass, "info", logger_info, 1);
    rb_define_method(LoggerClass, "debug", logger_debug, 1);
    rb_define_method(LoggerClass, "trace", logger_trace, 1);
    rb_define_attr(LoggerClass, "level", 1, 0);
    rb_define_attr(LoggerClass, "internal", 1, 1);
}
