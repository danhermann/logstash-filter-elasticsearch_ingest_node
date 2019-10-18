package org.logstashplugins;

import org.joda.time.DateTime;
import org.jruby.Ruby;
import org.jruby.RubyBignum;
import org.jruby.RubyBoolean;
import org.jruby.RubyFixnum;
import org.jruby.RubyFloat;
import org.jruby.RubyString;
import org.junit.Assert;
import org.junit.Test;
import org.logstash.Event;
import org.logstash.Timestamp;
import org.logstash.ext.JrubyTimestampExtLibrary;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class IngestMarshallerTest {

    @Test
    public void testRoundtrip() {
        Event original = getEvent();
        Event roundtrip = new org.logstash.Event();
        IngestMarshaller.toEvent(IngestMarshaller.toDocument(original), roundtrip);

        for (String key : original.toMap().keySet()) {
            Assert.assertEquals(original.getField(key), roundtrip.getField(key));
        }
    }

    private static org.logstash.Event getEvent() {
        Event original = new org.logstash.Event();
        Ruby ruby = Ruby.getGlobalRuntime();
        original.setField("ruby_string", RubyString.newString(ruby, "foo"));
        original.setField("ruby_fixnum", RubyFixnum.five(ruby));
        original.setField("ruby_timestamp",
                JrubyTimestampExtLibrary.RubyTimestamp.newRubyTimestamp(ruby, new Timestamp()));
        original.setField("ruby_float", RubyFloat.newFloat(ruby, 3.14));
        original.setField("ruby_boolean", RubyBoolean.newBoolean(ruby, true));
        original.setField("ruby_bignum", RubyBignum.long2big(43L));

        original.setField("float", 3.16f);
        original.setField("double", 3.17d);
        original.setField("bigint", BigInteger.valueOf(44L));
        original.setField("bigdecimal", BigDecimal.valueOf(22.12));
        original.setField("long_value", 42L);
        original.setField("int", 45);
        original.setField("short", (short)21);
        original.setField("byte", (byte)23);
        original.setField("boolean", true);
        original.setField("datetime", DateTime.now());
        original.setField("date", new Date());

        original.setField("list_of_strings", Arrays.asList("foo", "bar", "baz"));
        Map<String, Object> eventData = new HashMap<>();
        eventData.put("foo", 31);
        eventData.put("bar", 3.17);
        eventData.put("baz", "arg");
        original.setField("map", eventData);
        return original;
    }

}
