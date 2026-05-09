package br.com.wagenestetica.banco;

import android.Manifest;
import android.app.Activity;
import android.content.Intent;
import android.content.SharedPreferences;
import android.content.pm.PackageManager;
import android.graphics.Color;
import android.net.Uri;
import android.os.Build;
import android.os.Bundle;
import android.provider.MediaStore;
import android.view.Gravity;
import android.view.View;
import android.view.Window;
import android.widget.Button;
import android.widget.EditText;
import android.widget.FrameLayout;
import android.widget.LinearLayout;
import android.widget.ProgressBar;
import android.widget.ScrollView;
import android.widget.TextView;
import android.widget.Toast;

import androidx.core.content.FileProvider;

import org.mindrot.jbcrypt.BCrypt;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

public class MainActivity extends Activity {
    private static final int CAMERA_REQUEST = 2401;
    private static final int PERMISSION_REQUEST = 2402;
    private static final int BG = Color.rgb(11, 11, 11);
    private static final int SURFACE = Color.rgb(17, 24, 39);
    private static final int SURFACE_SOFT = Color.rgb(31, 41, 55);
    private static final int GOLD = Color.rgb(250, 204, 21);
    private static final int TEXT = Color.rgb(249, 250, 251);
    private static final int MUTED = Color.rgb(156, 163, 175);
    private static final int DANGER = Color.rgb(239, 68, 68);

    private final SupabaseClient supabase = new SupabaseClient();
    private SharedPreferences preferences;
    private LinearLayout root;
    private LinearLayout sidebar;
    private FrameLayout content;
    private ProgressBar progress;
    private String activeScreen = "dashboard";
    private String lastServiceId = "";
    private File pendingPhoto;
    private AppUser currentUser;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        configureSystemBars();
        requestRuntimePermissions();
        preferences = getSharedPreferences("supabase", MODE_PRIVATE);
        loadSupabaseConfig();

        if (!supabase.isConfigured()) {
            showConfigSetup();
            return;
        }
        showLogin();
    }

    private void loadSupabaseConfig() {
        String url = preferences.getString("url", BuildConfig.SUPABASE_URL);
        String anonKey = preferences.getString("anon_key", BuildConfig.SUPABASE_ANON_KEY);
        supabase.configure(url, anonKey);
    }

    private void saveSupabaseConfig(String url, String anonKey) {
        preferences.edit()
            .putString("url", url.trim())
            .putString("anon_key", anonKey.trim())
            .apply();
        supabase.configure(url, anonKey);
    }

    private void configureSystemBars() {
        Window window = getWindow();
        window.setStatusBarColor(BG);
        window.setNavigationBarColor(BG);
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.M) {
            window.getDecorView().setSystemUiVisibility(0);
        }
    }

    private void requestRuntimePermissions() {
        if (Build.VERSION.SDK_INT < Build.VERSION_CODES.M) {
            return;
        }
        if (checkSelfPermission(Manifest.permission.CAMERA) != PackageManager.PERMISSION_GRANTED) {
            requestPermissions(new String[]{Manifest.permission.CAMERA}, PERMISSION_REQUEST);
        }
    }

    private void buildShell() {
        root = new LinearLayout(this);
        root.setOrientation(LinearLayout.HORIZONTAL);
        root.setBackgroundColor(BG);

        sidebar = new LinearLayout(this);
        sidebar.setOrientation(LinearLayout.VERTICAL);
        sidebar.setPadding(dp(14), dp(18), dp(14), dp(18));
        sidebar.setBackgroundColor(SURFACE);
        root.addView(sidebar, new LinearLayout.LayoutParams(dp(214), LinearLayout.LayoutParams.MATCH_PARENT));

        LinearLayout main = new LinearLayout(this);
        main.setOrientation(LinearLayout.VERTICAL);
        main.setBackgroundColor(BG);
        root.addView(main, new LinearLayout.LayoutParams(0, LinearLayout.LayoutParams.MATCH_PARENT, 1));

        progress = new ProgressBar(this, null, android.R.attr.progressBarStyleHorizontal);
        progress.setVisibility(View.GONE);
        main.addView(progress, new LinearLayout.LayoutParams(LinearLayout.LayoutParams.MATCH_PARENT, dp(4)));

        content = new FrameLayout(this);
        main.addView(content, new LinearLayout.LayoutParams(LinearLayout.LayoutParams.MATCH_PARENT, 0, 1));

        setContentView(root);
        renderSidebar();
    }

    private void renderSidebar() {
        sidebar.removeAllViews();
        TextView brand = text("Wagen Estetica", 20, GOLD, true);
        TextView subtitle = text(currentUser == null ? "App Android nativo" : currentUser.nome + " / " + currentUser.perfil, 12, MUTED, false);
        sidebar.addView(brand);
        sidebar.addView(subtitle);
        sidebar.addView(spacer(18));
        addNav("dashboard", "Painel");
        addNav("clientes", "Clientes");
        addNav("atendimentos", "Atendimentos");
        addNav("foto", "Fotos");
        addNav("config", "Conexao");
        sidebar.addView(new View(this), new LinearLayout.LayoutParams(1, 0, 1));
        Button logout = button("Sair", DANGER);
        logout.setOnClickListener(v -> {
            currentUser = null;
            showLogin();
        });
        sidebar.addView(logout);
    }

    private void addNav(String screen, String label) {
        Button nav = button(label, screen.equals(activeScreen) ? GOLD : SURFACE_SOFT);
        nav.setTextColor(screen.equals(activeScreen) ? BG : TEXT);
        nav.setOnClickListener(v -> navigate(screen));
        sidebar.addView(nav);
    }

    private void navigate(String screen) {
        activeScreen = screen;
        renderSidebar();
        if ("dashboard".equals(screen)) {
            showDashboard();
        } else if ("clientes".equals(screen)) {
            showClients();
        } else if ("atendimentos".equals(screen)) {
            showServices();
        } else if ("foto".equals(screen)) {
            showPhotoScreen();
        } else {
            showConfig();
        }
    }

    private void showConfigSetup() {
        LinearLayout box = page("Configurar Supabase");
        EditText url = input("SUPABASE_URL");
        EditText anonKey = input("SUPABASE_ANON_KEY");
        url.setText(supabase.getBaseUrl());
        anonKey.setText(supabase.hasAnonKey() ? supabase.getAnonKey() : "");
        Button save = button("Salvar e entrar", GOLD);
        save.setOnClickListener(v -> {
            saveSupabaseConfig(url.getText().toString(), anonKey.getText().toString());
            if (!supabase.isConfigured()) {
                toast("Informe a URL e a anon key do Supabase.");
                return;
            }
            showLogin();
        });
        box.addView(paragraph("Informe os dados publicos do seu projeto Supabase. O app salva isso no celular e nao depende do Flask."));
        box.addView(url);
        box.addView(anonKey);
        box.addView(save);
        box.addView(paragraph("Use a anon public key. Nao use service role key nem senha do banco no aplicativo."));
        setContentView(wrap(box));
    }

    private void showLogin() {
        LinearLayout box = page("Acesso ao sistema");
        EditText usuario = input("Usuario");
        EditText password = input("Senha");
        password.setInputType(0x00000081);
        Button login = button("Entrar", GOLD);
        login.setOnClickListener(v -> runAsync(() -> {
            currentUser = supabase.loginUsuario(usuario.getText().toString(), password.getText().toString());
            runOnUiThread(() -> {
                buildShell();
                navigate("dashboard");
            });
        }));
        box.addView(usuario);
        box.addView(password);
        box.addView(login);
        box.addView(paragraph("Use o mesmo usuario e senha cadastrados no site."));
        setContentView(wrap(box));
    }

    private void showDashboard() {
        LinearLayout page = page("Painel");
        page.addView(paragraph("Resumo direto do Supabase, sem Flask e sem WebView."));
        LinearLayout grid = new LinearLayout(this);
        grid.setOrientation(LinearLayout.VERTICAL);
        page.addView(grid);
        setContentView(root);
        setContent(page);
        runAsync(() -> {
            int empresaId = empresaAtual();
            int clientes = supabase.countByEmpresa("clientes", empresaId);
            int veiculos = supabase.countByEmpresa("veiculos", empresaId);
            int andamento = supabase.countFilteredByEmpresa("servicos", empresaId, "status", "EM ANDAMENTO");
            runOnUiThread(() -> {
                grid.removeAllViews();
                grid.addView(metric("Clientes", String.valueOf(clientes)));
                grid.addView(metric("Veiculos", String.valueOf(veiculos)));
                grid.addView(metric("Em andamento", String.valueOf(andamento)));
            });
        });
    }

    private void showClients() {
        LinearLayout page = page("Clientes");
        EditText nome = input("Nome");
        EditText telefone = input("Telefone");
        EditText placa = input("Placa principal");
        Button salvar = button("Criar cliente", GOLD);
        LinearLayout lista = new LinearLayout(this);
        lista.setOrientation(LinearLayout.VERTICAL);
        salvar.setOnClickListener(v -> runAsync(() -> {
            JSONObject cliente = new JSONObject();
            cliente.put("empresa_id", empresaAtual());
            cliente.put("nome", value(nome, "Sem nome"));
            cliente.put("telefone", telefone.getText().toString().trim());
            cliente.put("placa_principal", placa.getText().toString().trim().toUpperCase(Locale.ROOT));
            supabase.insert("clientes", cliente);
            runOnUiThread(() -> {
                toast("Cliente criado.");
                showClients();
            });
        }));
        page.addView(nome);
        page.addView(telefone);
        page.addView(placa);
        page.addView(salvar);
        page.addView(section("Ultimos clientes"));
        page.addView(lista);
        setContent(page);
        runAsync(() -> {
            JSONArray dados = supabase.listByEmpresa("clientes", empresaAtual(), "id,nome,telefone,placa_principal", "id.desc", 20);
            runOnUiThread(() -> renderRows(lista, dados, new String[]{"id", "nome", "telefone", "placa_principal"}));
        });
    }

    private void showServices() {
        LinearLayout page = page("Atendimentos");
        EditText placa = input("Placa");
        EditText modelo = input("Modelo");
        EditText cor = input("Cor");
        EditText valor = input("Valor");
        Button criar = button("Criar atendimento", GOLD);
        LinearLayout lista = new LinearLayout(this);
        lista.setOrientation(LinearLayout.VERTICAL);
        criar.setOnClickListener(v -> runAsync(() -> {
            String placaValor = placa.getText().toString().trim().toUpperCase(Locale.ROOT);
            int veiculoId = criarOuAtualizarVeiculo(placaValor, modelo.getText().toString(), cor.getText().toString());
            JSONObject servico = new JSONObject();
            servico.put("empresa_id", 1);
            servico.put("veiculo_id", veiculoId);
            servico.put("status", "EM ANDAMENTO");
            servico.put("entrada", nowIso());
            servico.put("valor", parseMoney(valor.getText().toString()));
            supabase.insert("servicos", servico);
            runOnUiThread(() -> {
                toast("Atendimento criado.");
                showServices();
            });
        }));
        page.addView(placa);
        page.addView(modelo);
        page.addView(cor);
        page.addView(valor);
        page.addView(criar);
        page.addView(section("Atendimentos recentes"));
        page.addView(lista);
        setContent(page);
        runAsync(() -> {
            JSONArray dados = supabase.listByEmpresa("servicos", empresaAtual(), "id,status,entrada,valor,veiculo_id", "id.desc", 20);
            runOnUiThread(() -> renderRows(lista, dados, new String[]{"id", "status", "entrada", "valor", "veiculo_id"}));
        });
    }

    private int criarOuAtualizarVeiculo(String placa, String modelo, String cor) throws Exception {
        JSONArray existentes = supabase.filterByEmpresa("veiculos", "id", empresaAtual(), "placa", placa, 1);
        if (existentes.length() > 0) {
            return existentes.getJSONObject(0).getInt("id");
        }
        JSONObject veiculo = new JSONObject();
        veiculo.put("empresa_id", empresaAtual());
        veiculo.put("placa", placa);
        veiculo.put("modelo", modelo);
        veiculo.put("cor", cor);
        JSONObject criado = supabase.insertReturning("veiculos", veiculo);
        return criado.getInt("id");
    }

    private void showPhotoScreen() {
        LinearLayout page = page("Fotos");
        EditText serviceId = input("ID do atendimento");
        if (!lastServiceId.isEmpty()) {
            serviceId.setText(lastServiceId);
        }
        Button camera = button("Tirar foto", GOLD);
        camera.setOnClickListener(v -> {
            lastServiceId = serviceId.getText().toString().trim();
            openCamera();
        });
        page.addView(serviceId);
        page.addView(camera);
        page.addView(paragraph("A foto e enviada para o bucket `fotos` no Supabase Storage e registrada na tabela `fotos`."));
        setContent(page);
    }

    private void openCamera() {
        try {
            File dir = new File(getCacheDir(), "camera");
            if (!dir.exists()) {
                dir.mkdirs();
            }
            pendingPhoto = new File(dir, "foto_" + System.currentTimeMillis() + ".jpg");
            Uri uri = FileProvider.getUriForFile(this, getPackageName() + ".files", pendingPhoto);
            Intent intent = new Intent(MediaStore.ACTION_IMAGE_CAPTURE);
            intent.putExtra(MediaStore.EXTRA_OUTPUT, uri);
            intent.addFlags(Intent.FLAG_GRANT_WRITE_URI_PERMISSION | Intent.FLAG_GRANT_READ_URI_PERMISSION);
            startActivityForResult(intent, CAMERA_REQUEST);
        } catch (Exception error) {
            toast("Nao foi possivel abrir a camera: " + error.getMessage());
        }
    }

    @Override
    protected void onActivityResult(int requestCode, int resultCode, Intent data) {
        super.onActivityResult(requestCode, resultCode, data);
        if (requestCode == CAMERA_REQUEST && resultCode == RESULT_OK && pendingPhoto != null) {
            runAsync(() -> {
                String path = "empresa_" + empresaAtual() + "/" + pendingPhoto.getName();
                supabase.uploadFile("fotos", path, pendingPhoto, "image/jpeg");
                JSONObject foto = new JSONObject();
                foto.put("empresa_id", empresaAtual());
                foto.put("servico_id", parseInt(lastServiceId, 0));
                foto.put("tipo", "app_android");
                foto.put("caminho", path);
                foto.put("mime_type", "image/jpeg");
                foto.put("arquivo_nome", pendingPhoto.getName());
                foto.put("criado_em", nowIso());
                supabase.insert("fotos", foto);
                runOnUiThread(() -> toast("Foto enviada para o Supabase."));
            });
        }
    }

    private void showConfig() {
        LinearLayout page = page("Conexao");
        EditText url = input("SUPABASE_URL");
        EditText anonKey = input("SUPABASE_ANON_KEY");
        url.setText(supabase.getBaseUrl());
        anonKey.setText(supabase.hasAnonKey() ? supabase.getAnonKey() : "");
        Button save = button("Salvar conexao", GOLD);
        save.setOnClickListener(v -> {
            saveSupabaseConfig(url.getText().toString(), anonKey.getText().toString());
            toast("Conexao salva.");
        });
        page.addView(url);
        page.addView(anonKey);
        page.addView(save);
        page.addView(paragraph("Este APK nao usa Flask. Ele chama PostgREST e Storage diretamente e autentica pelo usuario do sistema."));
        page.addView(paragraph("Para dados reais, configure RLS/policies no Supabase. Use anon key, nunca service role key."));
        setContent(page);
    }

    private int empresaAtual() {
        return currentUser == null ? 1 : currentUser.empresaId;
    }

    private LinearLayout wrap(View child) {
        LinearLayout wrapper = new LinearLayout(this);
        wrapper.setOrientation(LinearLayout.VERTICAL);
        wrapper.setGravity(Gravity.CENTER);
        wrapper.setPadding(dp(20), dp(20), dp(20), dp(20));
        wrapper.setBackgroundColor(BG);
        wrapper.addView(child, new LinearLayout.LayoutParams(LinearLayout.LayoutParams.MATCH_PARENT, LinearLayout.LayoutParams.WRAP_CONTENT));
        return wrapper;
    }

    private LinearLayout page(String title) {
        LinearLayout page = new LinearLayout(this);
        page.setOrientation(LinearLayout.VERTICAL);
        page.setPadding(dp(18), dp(18), dp(18), dp(18));
        page.setBackgroundColor(BG);
        page.addView(text(title, 24, TEXT, true));
        page.addView(spacer(8));
        return page;
    }

    private void setContent(LinearLayout page) {
        ScrollView scroll = new ScrollView(this);
        scroll.addView(page);
        content.removeAllViews();
        content.addView(scroll);
    }

    private TextView text(String value, int sp, int color, boolean bold) {
        TextView tv = new TextView(this);
        tv.setText(value);
        tv.setTextSize(sp);
        tv.setTextColor(color);
        tv.setPadding(0, dp(4), 0, dp(4));
        if (bold) {
            tv.setTypeface(android.graphics.Typeface.DEFAULT_BOLD);
        }
        return tv;
    }

    private TextView paragraph(String value) {
        TextView tv = text(value, 14, MUTED, false);
        tv.setPadding(0, dp(8), 0, dp(8));
        return tv;
    }

    private TextView section(String value) {
        TextView tv = text(value, 18, GOLD, true);
        tv.setPadding(0, dp(18), 0, dp(8));
        return tv;
    }

    private EditText input(String hint) {
        EditText input = new EditText(this);
        input.setHint(hint);
        input.setHintTextColor(MUTED);
        input.setTextColor(TEXT);
        input.setSingleLine(true);
        input.setPadding(dp(12), dp(10), dp(12), dp(10));
        input.setBackgroundColor(SURFACE_SOFT);
        LinearLayout.LayoutParams lp = new LinearLayout.LayoutParams(LinearLayout.LayoutParams.MATCH_PARENT, LinearLayout.LayoutParams.WRAP_CONTENT);
        lp.setMargins(0, dp(8), 0, dp(8));
        input.setLayoutParams(lp);
        return input;
    }

    private Button button(String label, int color) {
        Button button = new Button(this);
        button.setText(label);
        button.setTextColor(color == GOLD ? BG : TEXT);
        button.setBackgroundColor(color);
        LinearLayout.LayoutParams lp = new LinearLayout.LayoutParams(LinearLayout.LayoutParams.MATCH_PARENT, LinearLayout.LayoutParams.WRAP_CONTENT);
        lp.setMargins(0, dp(6), 0, dp(6));
        button.setLayoutParams(lp);
        return button;
    }

    private View spacer(int heightDp) {
        View view = new View(this);
        view.setLayoutParams(new LinearLayout.LayoutParams(1, dp(heightDp)));
        return view;
    }

    private View metric(String label, String value) {
        LinearLayout card = new LinearLayout(this);
        card.setOrientation(LinearLayout.VERTICAL);
        card.setPadding(dp(14), dp(12), dp(14), dp(12));
        card.setBackgroundColor(SURFACE);
        LinearLayout.LayoutParams lp = new LinearLayout.LayoutParams(LinearLayout.LayoutParams.MATCH_PARENT, LinearLayout.LayoutParams.WRAP_CONTENT);
        lp.setMargins(0, dp(8), 0, dp(8));
        card.setLayoutParams(lp);
        card.addView(text(label, 13, MUTED, false));
        card.addView(text(value, 22, TEXT, true));
        return card;
    }

    private void renderRows(LinearLayout container, JSONArray rows, String[] fields) {
        container.removeAllViews();
        if (rows.length() == 0) {
            container.addView(paragraph("Nenhum registro encontrado."));
            return;
        }
        for (int i = 0; i < rows.length(); i++) {
            JSONObject row = rows.optJSONObject(i);
            if (row == null) {
                continue;
            }
            LinearLayout card = new LinearLayout(this);
            card.setOrientation(LinearLayout.VERTICAL);
            card.setPadding(dp(12), dp(10), dp(12), dp(10));
            card.setBackgroundColor(SURFACE);
            LinearLayout.LayoutParams lp = new LinearLayout.LayoutParams(LinearLayout.LayoutParams.MATCH_PARENT, LinearLayout.LayoutParams.WRAP_CONTENT);
            lp.setMargins(0, dp(6), 0, dp(6));
            card.setLayoutParams(lp);
            for (String field : fields) {
                card.addView(text(field + ": " + row.optString(field, "-"), 13, TEXT, false));
            }
            container.addView(card);
        }
    }

    private void runAsync(Task task) {
        progressOn(true);
        new Thread(() -> {
            try {
                task.run();
            } catch (Exception error) {
                runOnUiThread(() -> toast(error.getMessage()));
            } finally {
                progressOn(false);
            }
        }).start();
    }

    private void progressOn(boolean visible) {
        runOnUiThread(() -> {
            if (progress != null) {
                progress.setVisibility(visible ? View.VISIBLE : View.GONE);
            }
        });
    }

    private void toast(String value) {
        Toast.makeText(this, value, Toast.LENGTH_LONG).show();
    }

    private int dp(int value) {
        return Math.round(value * getResources().getDisplayMetrics().density);
    }

    private String value(EditText input, String fallback) {
        String value = input.getText().toString().trim();
        return value.isEmpty() ? fallback : value;
    }

    private boolean empty(String value) {
        return value == null || value.trim().isEmpty();
    }

    private int parseInt(String value, int fallback) {
        try {
            return Integer.parseInt(value.trim());
        } catch (Exception ignored) {
            return fallback;
        }
    }

    private double parseMoney(String value) {
        try {
            return Double.parseDouble(value.trim().replace(",", "."));
        } catch (Exception ignored) {
            return 0.0;
        }
    }

    private String nowIso() {
        return new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss", Locale.US).format(new Date());
    }

    private interface Task {
        void run() throws Exception;
    }

    private static class AppUser {
        final int id;
        final int empresaId;
        final String usuario;
        final String nome;
        final String perfil;

        AppUser(JSONObject row) {
            id = row.optInt("id", 0);
            empresaId = Math.max(1, row.optInt("empresa_id", 1));
            usuario = row.optString("usuario", "");
            nome = row.optString("nome", usuario);
            perfil = row.optString("perfil", "funcionario");
        }
    }

    private static class SupabaseClient {
        private String baseUrl = "";
        private String anonKey = "";

        void configure(String url, String key) {
            baseUrl = trimTrailingSlash(url);
            anonKey = key == null ? "" : key.trim();
        }

        boolean isConfigured() {
            return !baseUrl.isEmpty() && !anonKey.isEmpty();
        }

        String getBaseUrl() {
            return baseUrl;
        }

        String getAnonKey() {
            return anonKey;
        }

        boolean hasAnonKey() {
            return !anonKey.isEmpty();
        }

        AppUser loginUsuario(String usuario, String password) throws Exception {
            String usuarioNormalizado = usuario == null ? "" : usuario.trim();
            if (usuarioNormalizado.isEmpty() || password == null || password.isEmpty()) {
                throw new Exception("Informe usuario e senha.");
            }
            String select = "id,empresa_id,usuario,nome,perfil,ativo,senha";
            String path = "/rest/v1/usuarios?select=" + url(select) + "&usuario=eq." + url(usuarioNormalizado) + "&limit=1";
            JSONArray rows = new JSONArray(request("GET", path, null, false, "application/json"));
            if (rows.length() == 0) {
                throw new Exception("Usuario ou senha invalidos.");
            }
            JSONObject row = rows.getJSONObject(0);
            if (row.optInt("ativo", 1) == 0) {
                throw new Exception("Usuario inativo.");
            }
            if (!verificarSenha(password, row.optString("senha", ""))) {
                throw new Exception("Usuario ou senha invalidos.");
            }
            return new AppUser(row);
        }

        int countByEmpresa(String table, int empresaId) throws Exception {
            HttpResult result = requestRaw("GET", "/rest/v1/" + table + "?select=id&empresa_id=eq." + empresaId, null, false, "application/json", "exact");
            return parseCount(result.contentRange);
        }

        int countFilteredByEmpresa(String table, int empresaId, String field, String value) throws Exception {
            HttpResult result = requestRaw("GET", "/rest/v1/" + table + "?select=id&empresa_id=eq." + empresaId + "&" + field + "=eq." + url(value), null, false, "application/json", "exact");
            return parseCount(result.contentRange);
        }

        JSONArray listByEmpresa(String table, int empresaId, String select, String order, int limit) throws Exception {
            String path = "/rest/v1/" + table + "?select=" + url(select) + "&empresa_id=eq." + empresaId + "&order=" + url(order) + "&limit=" + limit;
            return new JSONArray(request("GET", path, null, false, "application/json"));
        }

        JSONArray filterByEmpresa(String table, String select, int empresaId, String field, String value, int limit) throws Exception {
            String path = "/rest/v1/" + table + "?select=" + url(select) + "&empresa_id=eq." + empresaId + "&" + field + "=eq." + url(value) + "&limit=" + limit;
            return new JSONArray(request("GET", path, null, false, "application/json"));
        }

        void insert(String table, JSONObject payload) throws Exception {
            request("POST", "/rest/v1/" + table, payload.toString(), false, "application/json");
        }

        JSONObject insertReturning(String table, JSONObject payload) throws Exception {
            HttpResult result = requestRaw("POST", "/rest/v1/" + table + "?select=*", payload.toString(), false, "application/json", null);
            JSONArray rows = new JSONArray(result.body);
            if (rows.length() == 0) {
                throw new Exception("Supabase nao retornou o registro criado.");
            }
            return rows.getJSONObject(0);
        }

        void uploadFile(String bucket, String path, File file, String mimeType) throws Exception {
            HttpURLConnection conn = open("POST", "/storage/v1/object/" + bucket + "/" + path, false);
            conn.setRequestProperty("Content-Type", mimeType);
            conn.setRequestProperty("x-upsert", "true");
            conn.setDoOutput(true);
            try (OutputStream out = new BufferedOutputStream(conn.getOutputStream());
                 InputStream in = new BufferedInputStream(new FileInputStream(file))) {
                byte[] buffer = new byte[8192];
                int read;
                while ((read = in.read(buffer)) != -1) {
                    out.write(buffer, 0, read);
                }
            }
            readResponse(conn);
        }

        private String request(String method, String path, String body, boolean auth, String contentType) throws Exception {
            return requestRaw(method, path, body, auth, contentType, null).body;
        }

        private HttpResult requestRaw(String method, String path, String body, boolean auth, String contentType, String count) throws Exception {
            HttpURLConnection conn = open(method, path, auth);
            conn.setRequestProperty("Content-Type", contentType);
            if (count != null) {
                conn.setRequestProperty("Prefer", "count=" + count);
                conn.setRequestProperty("Range-Unit", "items");
                conn.setRequestProperty("Range", "0-0");
            } else if ("POST".equals(method)) {
                conn.setRequestProperty("Prefer", "return=representation");
            }
            if (body != null) {
                conn.setDoOutput(true);
                try (OutputStream out = conn.getOutputStream()) {
                    out.write(body.getBytes(StandardCharsets.UTF_8));
                }
            }
            return readResponse(conn);
        }

        private HttpURLConnection open(String method, String path, boolean auth) throws Exception {
            HttpURLConnection conn = (HttpURLConnection) new URL(baseUrl + path).openConnection();
            conn.setRequestMethod(method);
            conn.setRequestProperty("apikey", anonKey);
            conn.setRequestProperty("Authorization", "Bearer " + anonKey);
            conn.setConnectTimeout(15000);
            conn.setReadTimeout(30000);
            return conn;
        }

        private HttpResult readResponse(HttpURLConnection conn) throws Exception {
            int status = conn.getResponseCode();
            InputStream stream = status >= 200 && status < 300 ? conn.getInputStream() : conn.getErrorStream();
            String body = readAll(stream);
            if (status < 200 || status >= 300) {
                if (status == 402) {
                    throw new Exception(
                        "Projeto Supabase restrito. Verifique Billing/Usage no painel do Supabase: pode ser cota excedida ou pagamento pendente."
                    );
                }
                throw new Exception("Supabase HTTP " + status + ": " + body);
            }
            return new HttpResult(body, conn.getHeaderField("Content-Range"));
        }

        private static String readAll(InputStream stream) throws Exception {
            if (stream == null) {
                return "";
            }
            StringBuilder out = new StringBuilder();
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    out.append(line);
                }
            }
            return out.toString();
        }

        private static int parseCount(String contentRange) {
            if (contentRange == null || !contentRange.contains("/")) {
                return 0;
            }
            try {
                return Integer.parseInt(contentRange.substring(contentRange.indexOf("/") + 1).trim());
            } catch (Exception ignored) {
                return 0;
            }
        }

        private static String trimTrailingSlash(String value) {
            String out = value == null ? "" : value.trim();
            while (out.endsWith("/")) {
                out = out.substring(0, out.length() - 1);
            }
            return out;
        }

        private static String url(String value) {
            return Uri.encode(value == null ? "" : value);
        }

        private static boolean verificarSenha(String password, String saved) {
            if (saved == null || saved.trim().isEmpty()) {
                return false;
            }
            String hash = saved.trim();
            if (hash.startsWith("$2b$") || hash.startsWith("$2y$")) {
                hash = "$2a$" + hash.substring(4);
            }
            if (hash.startsWith("$2a$")) {
                try {
                    return BCrypt.checkpw(password, hash);
                } catch (Exception ignored) {
                    return false;
                }
            }
            return password.equals(saved);
        }
    }

    private static class HttpResult {
        final String body;
        final String contentRange;

        HttpResult(String body, String contentRange) {
            this.body = body;
            this.contentRange = contentRange;
        }
    }
}
