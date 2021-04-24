defmodule MailchimpToSes do
  @moduledoc """
  `MailchimpToSes` converts a CSV exported by Mailchimp to a CSV importable as an AWS SES contact list.
  """
  @doc """
  Convert a CSV exported by Mailchimp to a CSV importable as an AWS SES contact list.

  Options:
  * `topic_preferences`: should be a list of tuples with string keys and value either `"OPT_IN"` or `:"OPT_OUT"`

  Mailchimp CSV has headers:
  * "Email Address"
  * "First Name"
  * "Last Name"
  * Address
  * "Phone Number"
  * Birthday
  * MEMBER_RATING
  * OPTIN_TIME
  * OPTIN_IP
  * CONFIRM_TIME
  * CONFIRM_IP
  * LATITUDE
  * LONGITUDE
  * GMTOFF
  * DSTOFF
  * TIMEZONE
  * CC
  * REGION
  * LAST_CHANGED
  * LEID
  * EUID
  * NOTES
  * TAGS

  AWS SES CSV has headers:
  * emailAddress
  * unsubscribeAll
  * attributesData
  * topicPreferences.<TOPIC_NAME>
  * topicPreferences.<TOPIC_NAME>

  """
  @type conversion_opts :: %{topic_preferences: [{String.t(), :opt_in | :opt_out}]}
  @spec convert(String.t(), String.t()) ::
          {:ok, String.t(), conversion_opts} | {:error, String.t()}
  def convert(input_filename, output_filename, opts \\ []) do
    input_stream =
      input_filename
      |> Path.expand(__DIR__)
      |> File.stream!([:utf8, :read_ahead], :line)

    Stream.take(input_stream, 1)

    output_stream = File.stream!(output_filename, [:utf8, :delayed_write], :line)

    topics = extract_topic_preferences(opts[:topic_preferences])

    input_stream
    |> CSV.decode(headers: true)
    |> Stream.map(fn {:ok, s} -> s end)
    |> Stream.map(&format_contact(&1, topics))
    |> CSV.encode(headers: true)
    |> Stream.into(output_stream)
    |> Stream.run()

    {:ok, output_filename}
  end

  defp format_contact(mailchimp_contact) do
    %{
      emailAddress: mailchimp_contact["Email Address"],
      unsubscribeAll: false,
      attributesData: nil
    }
  end

  defp format_contact(mailchimp_contact, []) do
    format_contact(mailchimp_contact)
  end

  defp format_contact(mailchimp_contact, topics) do
    format_contact(mailchimp_contact)
    |> Map.merge(topics)
  end

  defp extract_topic_preferences(nil), do: %{}

  defp extract_topic_preferences(topics) do
    Enum.reduce(topics, %{}, fn {topic_name, topic_value}, acc ->
      Map.merge(acc, %{("topicPreferences." <> topic_name) => topic_value})
    end)
  end
end
