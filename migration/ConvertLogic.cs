using System;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json;

namespace ConvertLogic
{
    public class convert
    {
        public static JArray add(JArray movies)
        {
            JArray ret_arr;
            ret_arr = movies;
            String json = "{'id':'123','dumy':124,'title':'testset'}";
            // Console.WriteLine(JsonConvert.DeserializeObject<JObject>(json));

            ret_arr.Add(JsonConvert.DeserializeObject<JObject>(json));
            return ret_arr;
        }
        public static JArray update(JArray movies)
        {
            JArray ret_arr = movies;

            foreach (JObject parsedObject in ret_arr.Children<JObject>())
            {
                int idx = ret_arr.IndexOf(parsedObject);
                Console.WriteLine(idx);
                foreach (JProperty parsedProperty in parsedObject.Properties())
                {
                    string key = parsedProperty.Name;
                    if (parsedProperty.Value.GetType() != typeof(JObject))
                    {
                        string value = (string)parsedProperty.Value;
                        if (key == "score")
                        {
                            if (Int32.Parse(value) > 50)
                            {

                                ret_arr[idx]["score"] = "high";

                            }
                            else
                            {
                                ret_arr[idx]["score"] = "low";
                            }
                        }
                    }
                }
            }

            return ret_arr;
        }
        public static JArray delete(JArray movies)
        {
            JArray ret_arr;
            ret_arr = movies;
            foreach (JObject parsedObject in ret_arr.Children<JObject>())
            {
                parsedObject.Remove("info");
            }

            return ret_arr;
        }
    }
}