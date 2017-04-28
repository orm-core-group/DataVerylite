using System;

namespace DataVeryLite.Aop
{
    [AopAttribute]
    public class AopProxy : ContextBoundObject
    {
        public virtual void InvokeSetProperty(string name, object value)
        {

        }

        public virtual void InvokeGetProperty(string name)
        {
        }

        public virtual void InvokeMethod(string name, object[] args)
        {

        }
    }
}
