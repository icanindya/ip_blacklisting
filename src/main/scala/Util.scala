import java.text.ParseException
import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.Date
import java.util.Locale;
import java.util.concurrent.TimeUnit

object Util {
  def dayDiff(d1: String , d2: String) : Long  = {
    var date1 = new SimpleDateFormat("yyyy-MM-dd").parse(d1);
    var date2 = new SimpleDateFormat("yyyy-MM-dd").parse(d2)
    Math.abs(TimeUnit.DAYS.convert(date2.getTime - date1.getTime, TimeUnit.MILLISECONDS));
  }
}