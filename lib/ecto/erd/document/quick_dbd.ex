defmodule Ecto.ERD.Document.QuickDBD do
  @moduledoc false
  alias Ecto.ERD.{Node, Field, Edge, Graph, Render}
  @behaviour Ecto.ERD.Document

  @impl true
  def schemaless?, do: true

  @impl true
  def render(%Graph{nodes: nodes, edges: edges}, _opts) do
    foreign_keys_mapping =
      Map.new(edges, fn %Edge{to: {to_source, _to_schema, {:field, to_field}}} = edge ->
        {{to_source, to_field}, edge}
      end)
    IO.inspect(edges)
    IO.inspect(foreign_keys_mapping, limit: :infinity)

    Enum.map_join(nodes, "\n\n", &render_node(&1, foreign_keys_mapping))
  end

  defp render_node(%Node{source: source, fields: fields}, foreign_keys_mapping) do
    get_fkey_edge = fn field_name -> foreign_keys_mapping[{source, field_name}] end

    [
      source,
      "\n---\n",
      fields
      |> Enum.map(&render_field(&1, get_fkey_edge))
      |> Enum.intersperse("\n")
    ]
  end

  defp render_field(%Field{name: name, type: type, primary?: primary?}, get_fkey_edge) do
    settings =
      if primary? do
        ["PK"]
      else
        case get_fkey_edge.(name) do
          %Edge{
            from: {from_source, _from_schema, {:field, from_field}},
            assoc_types: assoc_types
          } ->
            # operator =
            #   if {:has, :one} in assoc_types do
            #     "-"
            #   else
            #     ">-"
            #   end
            #
            # [
            #   "FK #{operator} #{Render.in_quotes(from_source)}.#{Render.in_quotes(from_field)}"
            # ]

            operator = ">-"

            result =
              if :belongs_to in assoc_types do
                if {:on_delete, :delete_all} in assoc_types do
                  [
                    "FK #{operator} #{Render.in_quotes(from_source)}.#{Render.in_quotes(from_field)}"
                  ]
                else
                  if {:on_delete, :nothing} in assoc_types do
                    []
                  else
                    [
                      "FK #{operator} #{Render.in_quotes(from_source)}.#{Render.in_quotes(from_field)}"
                    ]
                  end
                end
              else
                []
              end

            result

          _ ->
            []
        end
      end

    Enum.intersperse(
      [Render.in_quotes(name), Render.in_quotes(format_type(type)) | settings],
      " "
    )
  end

  defp format_type({:parameterized, Ecto.Enum, %{on_dump: on_dump}}) do
    "enum(#{Enum.join(Map.values(on_dump), ",")})"
  end

  defp format_type(type) do
    case Ecto.Type.type(type) do
      {:array, _t} -> "array"
      :id -> "integer"
      :identity -> "bigint"
      :binary_id -> "uuid"
      :string -> "varchar"
      :binary -> "bytea"
      :map -> "jsonb"
      # TODO: remove :embed support when apps in examples won't use legacy ecto version
      # for :map and legacy :embed. It is not written in code explicitly to shut up dialyzer
      {_, _} -> "jsonb"
      :time_usec -> "time"
      :utc_datetime -> "timestamp"
      :utc_datetime_usec -> "timestamp"
      :naive_datetime -> "timestamp"
      :naive_datetime_usec -> "timestamp"
      atom when is_atom(atom) -> Atom.to_string(atom)
    end
  end
end
